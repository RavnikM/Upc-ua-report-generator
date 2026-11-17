"""
Full Prototype & End-to-End Pipeline Verification Suite:
1. Non-Redundant Static Header Tags (`Batch_Number`, `Recipe_Name` read ONCE at cycle start,
   never repeated in 0.5s time-series polling).
2. 0.5s S7-1500 OPC-UA Bulk Polling -> SQLAlchemy Database Logging (`CycleTimeSeries`).
3. Watchdog CSV Alarm Folder Monitoring (`app/csv_watcher.py`) -> DB (`CycleAlarmLog`)
   -> Automatic move to `.bckup/` subfolder.
4. Automated Pandas + Matplotlib + Jinja2 + WeasyPrint PDF Report Generation (including
   one-shot Batch_Number and CSV-ingested alarms).
"""

import asyncio
from datetime import datetime, timezone
import logging
from pathlib import Path
import shutil
import sys

from sqlalchemy import delete, select

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.csv_watcher import AlarmCSVWatcherService
from app.database import engine, get_Session_context, init_db
from app.main import (
    create_report_schema,
    download_cycle_pdf,
    get_cycle_details,
    health_check,
    list_cycles,
    list_ingested_csv_alarms,
    list_report_schemas,
)
from app.models import (
    CycleAlarmLog,
    CycleRecord,
    CycleStatus,
    CycleTimeSeries,
    ReportSchema,
)
from app.opc_client import CyclePollerConfig, S7CyclePoller
from app.schemas import (
    AlarmRuleSchema,
    ChartElementSchema,
    ChartSeriesSchema,
    ParameterRowSchema,
    ParameterTableElementSchema,
    ReportLayoutConfig,
    ReportSchemaCreate,
)
from tests.mock_s7_server import MockS71500Server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logging.getLogger("asyncua").setLevel(logging.WARNING)
logging.getLogger("fontTools").setLevel(logging.WARNING)
logging.getLogger("weasyprint").setLevel(logging.WARNING)
logger = logging.getLogger("test_e2e_pipeline")


async def run_end_to_end_pipeline_test() -> None:
    """Execute the complete OPC-UA + Watchdog CSV + DB + WeasyPrint PDF prototype test."""
    logger.info("=" * 78)
    logger.info("PROTOTYPE E2E TEST: ONE-SHOT BATCH TAGS + WATCHDOG CSV (.bckup) + PDF REPORT")
    logger.info("=" * 78)

    await init_db()

    # Clean previous test records and test watched folder
    test_watch_dir = PROJECT_ROOT / "watched_alarms"
    if test_watch_dir.exists():
        shutil.rmtree(test_watch_dir)

    async with get_Session_context() as session:
        await session.execute(delete(CycleAlarmLog))
        await session.execute(delete(CycleTimeSeries))
        await session.execute(delete(CycleRecord))
        await session.execute(delete(ReportSchema))

    # 1. Create ReportSchema with `header_tags` (read ONCE) and `tags` (polled every 0.5s)
    layout_config = ReportLayoutConfig(
        header_title="Siemens S7-1500 Autoclave Production Report",
        machine_name="S7-1500 Autoclave Line #1",
        auto_print=True,
        printer_name="Factory_Floor_LaserJet_01",
        header_tags={
            "Batch_Number": 'ns=3;s="Cycle_Data"."Batch_Number"',
            "Recipe_Name": 'ns=3;s="Cycle_Data"."Recipe_Name"',
        },
        tags={
            "Temp_Zone1": 'ns=3;s="Cycle_Data"."Temp_Zone1"',
            "Temp_Zone2": 'ns=3;s="Cycle_Data"."Temp_Zone2"',
            "Pressure_Main": 'ns=3;s="Cycle_Data"."Pressure_Main"',
            "Motor_Speed_RPM": 'ns=3;s="Cycle_Data"."Motor_Speed_RPM"',
        },
        charts=[
            ChartElementSchema(
                id="zone_temps_chart",
                title="Heating Zone 1 & Zone 2 Temperatures (°C)",
                y_axis_label="Temperature (°C)",
                series=[
                    ChartSeriesSchema(
                        tag_name="Temp_Zone1",
                        label="Zone 1 Temp",
                        unit="°C",
                        color="#dc2626",
                    ),
                    ChartSeriesSchema(
                        tag_name="Temp_Zone2",
                        label="Zone 2 Temp",
                        unit="°C",
                        color="#2563eb",
                    ),
                ],
            ),
            ChartElementSchema(
                id="pressure_chart",
                title="Chamber Pressure Profile (bar)",
                y_axis_label="Pressure (bar)",
                series=[
                    ChartSeriesSchema(
                        tag_name="Pressure_Main",
                        label="Main Chamber Pressure",
                        unit="bar",
                        color="#16a34a",
                    ),
                ],
            ),
        ],
        parameter_tables=[
            ParameterTableElementSchema(
                id="summary_table_main",
                title="Cycle Process Parameter Summary",
                parameters=[
                    ParameterRowSchema(
                        tag_name="Temp_Zone1",
                        label="Zone 1 Temperature",
                        unit="°C",
                        aggregations=["min", "max", "avg"],
                        target_setpoint=185.0,
                        tolerance=6.0,
                    ),
                    ParameterRowSchema(
                        tag_name="Temp_Zone2",
                        label="Zone 2 Temperature",
                        unit="°C",
                        aggregations=["min", "max", "avg"],
                        target_setpoint=198.0,
                        tolerance=6.0,
                    ),
                    ParameterRowSchema(
                        tag_name="Pressure_Main",
                        label="Vessel Pressure",
                        unit="bar",
                        aggregations=["min", "max", "avg"],
                        target_setpoint=4.5,
                        tolerance=0.5,
                    ),
                    ParameterRowSchema(
                        tag_name="Motor_Speed_RPM",
                        label="Circulation Fan Speed",
                        unit="RPM",
                        aggregations=["min", "max", "avg"],
                    ),
                ],
            )
        ],
        alarms=[
            AlarmRuleSchema(
                id="alm_zone1_high",
                tag_name="Temp_Zone1",
                label="Zone 1 High Temperature Excursion (> 192.0 °C)",
                condition="gt",
                threshold=192.0,
                severity="WARNING",
            ),
        ],
    )

    create_payload = ReportSchemaCreate(
        report_name="Autoclave_Full_Production_Report",
        description="Prototype report with one-shot Batch_Number & Watchdog CSV alarms.",
        trigger_node='ns=3;s="Cycle_Control"."Cycle_Active"',
        schema_config=layout_config,
        is_active=True,
    )

    async with get_Session_context() as session:
        created_schema = await create_report_schema(create_payload, db=session)
        schema_id = created_schema.id
        header_nodes = created_schema.extract_header_nodes()
        polled_nodes = created_schema.extract_polled_nodes()

    assert "Batch_Number" in header_nodes and "Batch_Number" not in polled_nodes
    assert "Recipe_Name" in header_nodes and "Recipe_Name" not in polled_nodes
    assert len(polled_nodes) == 4

    poller_config = CyclePollerConfig(
        report_schema_id=schema_id,
        trigger_node_id='ns=3;s="Cycle_Control"."Cycle_Active"',
        header_nodes_map=header_nodes,
        data_nodes_map=polled_nodes,
        poll_interval=0.5,
        persist_to_db=True,
        auto_generate_pdf=True,
    )

    # 2. Start Mock S7-1500 Server, S7CyclePoller, and Watchdog AlarmCSVWatcherService
    endpoint = "opc.tcp://127.0.0.1:48430"
    async with MockS71500Server(endpoint=endpoint) as plc:
        poller = S7CyclePoller(endpoint_url=endpoint, config=poller_config)
        csv_watcher = AlarmCSVWatcherService(
            watch_dir=test_watch_dir,
            active_cycle_id_provider=lambda: poller.active_cycle_id,
        )
        await csv_watcher.start()
        poller_task = asyncio.create_task(poller.start())

        try:
            await asyncio.sleep(0.6)

            # --- CYCLE 1: Nominal Cycle (Batch_Number = BATCH-2026-001) ---
            logger.info("2a. Running Cycle #1 (Batch=BATCH-2026-001, No Alarms)...")
            await plc.update_process_values(
                step=1,
                recipe_name="RECIPE_NOMINAL_101",
                batch_number="BATCH-2026-001",
            )
            await plc.set_cycle_active(True)
            for step in range(2, 5):
                await asyncio.sleep(0.5)
                await plc.update_process_values(
                    step=step,
                    recipe_name="RECIPE_NOMINAL_101",
                    batch_number="BATCH-2026-001",
                )
            await asyncio.sleep(0.4)
            await plc.set_cycle_active(False)
            await asyncio.sleep(1.0)

            # --- CYCLE 2: Cycle with CSV Alarm Exported Mid-Cycle (Batch=BATCH-2026-002) ---
            logger.info("2b. Running Cycle #2 (Batch=BATCH-2026-002 + Dropping CSV Alarm File)...")
            await plc.update_process_values(
                step=5,
                recipe_name="RECIPE_EXCURSION_102",
                batch_number="BATCH-2026-002",
            )
            await plc.set_cycle_active(True)
            for step in range(6, 10):
                await asyncio.sleep(0.5)
                await plc.update_process_values(
                    step=step,
                    recipe_name="RECIPE_EXCURSION_102",
                    batch_number="BATCH-2026-002",
                )
                if step == 7:
                    # Drop an alarm CSV file into watched_alarms/ while Cycle #2 is ACTIVE!
                    now_iso = datetime.now(timezone.utc).isoformat()
                    csv_file = test_watch_dir / "s7_hmi_alarms_batch002.csv"
                    csv_file.write_text(
                        "timestamp,alarm_code,alarm_text,severity\n"
                        f"{now_iso},ALM_104,Zone 1 Thermocouple Deviation Warning,WARNING\n"
                        f"{now_iso},ALM_901,Chamber Over-Pressure Safety Valve Tripped,CRITICAL\n",
                        encoding="utf-8",
                    )
                    logger.info("Dropped CSV file '%s' into watched folder.", csv_file.name)

            await asyncio.sleep(0.5)
            await plc.set_cycle_active(False)

            # Wait for background PDF generation & Watchdog backup to complete
            await asyncio.sleep(2.2)

        finally:
            await poller.stop()
            poller_task.cancel()
            await asyncio.gather(poller_task, return_exceptions=True)
            await csv_watcher.stop()

    # 3. Verify Watchdog moved CSV file to .bckup/ and logged rows to DB
    backup_csv = test_watch_dir / ".bckup" / "s7_hmi_alarms_batch002.csv"
    assert backup_csv.is_file(), f"Expected CSV to be moved to {backup_csv}"
    assert not (test_watch_dir / "s7_hmi_alarms_batch002.csv").exists(), (
        "Original CSV file should no longer be in root watch folder"
    )
    logger.info("Verified Watchdog moved CSV to: %s", backup_csv)

    # 4. Verify Database records (Non-redundant Batch_Number, CSV alarms, and PDF reports)
    async with get_Session_context() as session:
        cycles = await list_cycles(
            report_schema_id=schema_id, cycle_status=None, limit=10, db=session
        )
        cycles_ordered = sorted(cycles, key=lambda c: c.id)
        assert len(cycles_ordered) == 2

        c1, c2 = cycles_ordered[0], cycles_ordered[1]
        assert c1.batch_number == "BATCH-2026-001"
        assert c1.header_data == {
            "Batch_Number": "BATCH-2026-001",
            "Recipe_Name": "RECIPE_NOMINAL_101",
        }
        assert c2.batch_number == "BATCH-2026-002"
        assert c2.header_data == {
            "Batch_Number": "BATCH-2026-002",
            "Recipe_Name": "RECIPE_EXCURSION_102",
        }

        # Verify that Batch_Number and Recipe_Name were NEVER polled in CycleTimeSeries!
        ts_tags_res = await session.execute(
            select(CycleTimeSeries.tag_name).distinct()
        )
        polled_tag_names = set(ts_tags_res.scalars().all())
        assert "Batch_Number" not in polled_tag_names
        assert "Recipe_Name" not in polled_tag_names
        assert polled_tag_names == {
            "Temp_Zone1",
            "Temp_Zone2",
            "Pressure_Main",
            "Motor_Speed_RPM",
        }
        logger.info(
            "Verified non-redundant 0.5s polling: only dynamic tags %s stored in CycleTimeSeries!",
            polled_tag_names,
        )

        # Verify CSV alarms in DB are linked to Cycle #2
        c2_details = await get_cycle_details(c2.id, db=session)
        assert len(c2_details["csv_alarms"]) == 2
        codes = {a["alarm_code"] for a in c2_details["csv_alarms"]}
        assert codes == {"ALM_104", "ALM_901"}

        # Verify generated PDFs
        for c in (c1, c2):
            assert c.status == CycleStatus.PRINTED
            assert c.pdf_file_path and Path(c.pdf_file_path).is_file()

    await engine.dispose()
    logger.info("=" * 78)
    logger.info("ALL PROTOTYPE E2E TESTS (ONE-SHOT BATCH + WATCHDOG CSV + PDF) PASSED!")
    logger.info("=" * 78)


if __name__ == "__main__":
    asyncio.run(run_end_to_end_pipeline_test())
