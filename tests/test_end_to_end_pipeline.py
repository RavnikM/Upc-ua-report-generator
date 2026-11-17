"""
Stage 3 & Full End-to-End Pipeline Verification Suite:
1. Web Report Designer Schema Creation (FastAPI / Pydantic)
2. 0.5s S7-1500 OPC-UA Bulk Polling -> SQLAlchemy Database Logging
3. Automatic Pandas + Matplotlib + Jinja2 + WeasyPrint PDF Report Generation
4. OS Printing (`win32print` / cross-platform handler)
5. FastAPI REST API Endpoint Verification
"""

import asyncio
import logging
from pathlib import Path
import sys

from sqlalchemy import delete, select

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.database import engine, get_Session_context, init_db
from app.main import (
    create_report_schema,
    download_cycle_pdf,
    get_cycle_details,
    health_check,
    list_cycles,
    list_report_schemas,
    trigger_cycle_report_endpoint,
)
from app.models import CycleRecord, CycleStatus, CycleTimeSeries, ReportSchema
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
    """Execute the complete OPC-UA -> Database -> PDF Report -> FastAPI pipeline."""
    logger.info("=" * 76)
    logger.info("FULL END-TO-END PIPELINE TEST: OPC-UA -> DB -> WEASYPRINT PDF -> FASTAPI")
    logger.info("=" * 76)

    await init_db()

    # Clean previous test records
    async with get_Session_context() as session:
        await session.execute(delete(CycleTimeSeries))
        await session.execute(delete(CycleRecord))
        await session.execute(delete(ReportSchema))

    # 1. Create ReportSchema via the FastAPI handler
    layout_config = ReportLayoutConfig(
        header_title="Siemens S7-1500 Autoclave Production Report",
        machine_name="S7-1500 Autoclave Line #1",
        auto_print=True,
        printer_name="Factory_Floor_LaserJet_01",
        tags={
            "Temp_Zone1": 'ns=3;s="Cycle_Data"."Temp_Zone1"',
            "Temp_Zone2": 'ns=3;s="Cycle_Data"."Temp_Zone2"',
            "Pressure_Main": 'ns=3;s="Cycle_Data"."Pressure_Main"',
            "Motor_Speed_RPM": 'ns=3;s="Cycle_Data"."Motor_Speed_RPM"',
            "Step_Counter": 'ns=3;s="Cycle_Data"."Step_Counter"',
            "Alarm_OverTemp": 'ns=3;s="Cycle_Data"."Alarm_OverTemp"',
            "Recipe_Name": 'ns=3;s="Cycle_Data"."Recipe_Name"',
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
                id="alm_overtemp_bit",
                tag_name="Alarm_OverTemp",
                label="PLC Over-Temperature Safety Interlock",
                condition="is_true",
                severity="CRITICAL",
            ),
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
        description="End-to-end automated report with Matplotlib charts and WeasyPrint PDF.",
        trigger_node='ns=3;s="Cycle_Control"."Cycle_Active"',
        schema_config=layout_config,
        is_active=True,
    )

    async with get_Session_context() as session:
        created_schema = await create_report_schema(create_payload, db=session)
        schema_id = created_schema.id
        poller_config = CyclePollerConfig(
            report_schema_id=created_schema.id,
            trigger_node_id=created_schema.trigger_node,
            data_nodes_map=created_schema.extract_polled_nodes(),
            poll_interval=0.5,
            persist_to_db=True,
            auto_generate_pdf=True,
        )

    logger.info("1. Created ReportSchema via FastAPI handler (id=%d)", schema_id)

    # 2. Run Mock S7-1500 Server + S7CyclePoller for 2 full production cycles
    endpoint = "opc.tcp://127.0.0.1:48430"
    async with MockS71500Server(endpoint=endpoint) as plc:
        poller = S7CyclePoller(endpoint_url=endpoint, config=poller_config)
        poller_task = asyncio.create_task(poller.start())

        try:
            await asyncio.sleep(0.6)

            # --- CYCLE 1: Nominal Cycle (4 samples @ 0.5s, no alarms) ---
            logger.info("2a. Running Cycle #1 (Nominal Operation, No Alarms)...")
            await plc.update_process_values(
                step=1, alarm_overtemp=False, recipe_name="RECIPE_NOMINAL_101"
            )
            await plc.set_cycle_active(True)
            for step in range(2, 5):
                await asyncio.sleep(0.5)
                await plc.update_process_values(
                    step=step, alarm_overtemp=False, recipe_name="RECIPE_NOMINAL_101"
                )
            await asyncio.sleep(0.4)
            await plc.set_cycle_active(False)

            # Wait briefly while Cycle 1 PDF generates in background
            await asyncio.sleep(1.0)

            # --- CYCLE 2: Excursion Cycle (Temp_Zone1 > 192°C & Alarm_OverTemp=True) ---
            logger.info("2b. Running Cycle #2 (High-Temp Excursion + Critical Alarm)...")
            await plc.update_process_values(
                step=5, alarm_overtemp=False, recipe_name="RECIPE_EXCURSION_102"
            )
            await plc.set_cycle_active(True)
            for step in range(6, 10):
                await asyncio.sleep(0.5)
                await plc.update_process_values(
                    step=step,
                    alarm_overtemp=(step >= 7),
                    recipe_name="RECIPE_EXCURSION_102",
                )
            await asyncio.sleep(0.4)
            await plc.set_cycle_active(False)

            # Wait for background PDF generation & OS print dispatch to finish
            await asyncio.sleep(2.0)

        finally:
            await poller.stop()
            poller_task.cancel()
            await asyncio.gather(poller_task, return_exceptions=True)

    # 3. Verify generated PDF reports and FastAPI endpoints
    async with get_Session_context() as session:
        health = await health_check()
        assert health["status"] == "ok"

        schemas_list = await list_report_schemas(active_only=True, db=session)
        assert len(schemas_list) == 1

        cycles = await list_cycles(
            report_schema_id=schema_id, cycle_status=None, limit=10, db=session
        )
        assert len(cycles) == 2, f"Expected 2 cycles, got {len(cycles)}"

        # Order chronologically (Cycle 1 then Cycle 2)
        cycles_ordered = sorted(cycles, key=lambda c: c.id)

        for idx, c in enumerate(cycles_ordered, start=1):
            details = await get_cycle_details(c.id, db=session)
            assert details["timeseries_points_count"] >= 21
            assert c.status == CycleStatus.PRINTED, (
                f"Expected Cycle {c.id} status=PRINTED, got {c.status}"
            )
            assert c.pdf_file_path is not None

            pdf_path = Path(c.pdf_file_path)
            assert pdf_path.is_file(), f"PDF file missing on disk: {pdf_path}"
            pdf_bytes = pdf_path.read_bytes()
            assert pdf_bytes.startswith(b"%PDF-1."), "File is not a valid PDF binary"
            assert len(pdf_bytes) > 15_000, (
                f"Expected rich PDF with embedded Matplotlib charts (>15KB), got {len(pdf_bytes)} bytes"
            )

            # Test FileResponse endpoint
            file_resp = await download_cycle_pdf(c.id, db=session)
            assert Path(file_resp.path) == pdf_path

            logger.info(
                "Verified Cycle #%d (id=%d): status=%s, points=%d, pdf='%s' (%d bytes)",
                idx,
                c.id,
                c.status.value,
                details["timeseries_points_count"],
                pdf_path.name,
                len(pdf_bytes),
            )

        # Also test manual report re-generation endpoint on Cycle #2
        regen_resp = await trigger_cycle_report_endpoint(
            cycles_ordered[1].id, force_print=True, db=session
        )
        assert regen_resp["triggered_alarms_count"] == 2, (
            f"Expected 2 triggered alarms on Cycle #2, got {regen_resp}"
        )
        logger.info(
            "Verified Manual Re-Generation Endpoint for Cycle #2: %s",
            regen_resp,
        )

    await engine.dispose()
    logger.info("=" * 76)
    logger.info("ALL STAGE 3 & END-TO-END PIPELINE TESTS PASSED SUCCESSFULLY!")
    logger.info("=" * 76)


if __name__ == "__main__":
    asyncio.run(run_end_to_end_pipeline_test())
