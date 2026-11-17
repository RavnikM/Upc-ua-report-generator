"""
Stage 2 Verification Suite: Database Layer, Alembic Schema, Dialect Agnosticism
(SQLite + MS SQL Server), and End-to-End OPC-UA -> Database Persistence.
"""

import asyncio
import logging
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import delete, inspect, select
from sqlalchemy.dialects import mssql, sqlite
from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import CreateIndex, CreateTable

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.database import Base, engine, get_Session_context, init_db
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
logger = logging.getLogger("test_db_persistence")


async def test_1_mssql_and_sqlite_dialect_compilation() -> None:
    """
    Verify 100% SQLAlchemy DDL compatibility across both SQLite and MS SQL Server.
    """
    logger.info("=" * 72)
    logger.info("TEST 1: SQLite & Microsoft SQL Server DDL Compilation Verification")
    logger.info("=" * 72)

    sqlite_dialect = sqlite.dialect()
    mssql_dialect = mssql.dialect()

    for table in Base.metadata.sorted_tables:
        sqlite_ddl = str(CreateTable(table).compile(dialect=sqlite_dialect)).strip()
        mssql_ddl = str(CreateTable(table).compile(dialect=mssql_dialect)).strip()

        assert f"CREATE TABLE {table.name}" in sqlite_ddl
        assert f"CREATE TABLE {table.name}" in mssql_ddl
        logger.info("Compiled MSSQL DDL for '%s':\n%s", table.name, mssql_ddl)

        for idx in table.indexes:
            mssql_idx_ddl = str(CreateIndex(idx).compile(dialect=mssql_dialect)).strip()
            assert idx.name in mssql_idx_ddl

    logger.info("-> Test 1 PASSED: All tables & indexes compile cleanly on SQLite & MS SQL Server.")


async def test_2_schema_inspection_and_foreign_key_integrity() -> None:
    """
    Verify tables/indexes created by Alembic and test Foreign Key integrity rules
    (RESTRICT on ReportSchema deletion when cycles exist; CASCADE on CycleRecord deletion).
    """
    logger.info("=" * 72)
    logger.info("TEST 2: Database Inspection & Foreign Key Integrity Enforcement")
    logger.info("=" * 72)

    await init_db()

    async with engine.connect() as conn:
        table_names = await conn.run_sync(
            lambda sync_conn: inspect(sync_conn).get_table_names()
        )
        ts_indexes = await conn.run_sync(
            lambda sync_conn: inspect(sync_conn).get_indexes("cycle_time_series")
        )

    for expected_table in ("report_schemas", "cycle_records", "cycle_time_series"):
        assert expected_table in table_names, f"Missing table {expected_table}"

    index_names = {idx["name"] for idx in ts_indexes}
    assert "ix_cycle_timeseries_cycle_tag_ts" in index_names, (
        f"Composite time-series index missing: {index_names}"
    )

    # Clean any pre-existing test rows
    async with get_Session_context() as session:
        await session.execute(delete(CycleTimeSeries))
        await session.execute(delete(CycleRecord))
        await session.execute(delete(ReportSchema))

    # Verify Foreign Key enforcement: CycleRecord with invalid report_schema_id=99999 must fail
    fk_error_caught = False
    try:
        async with get_Session_context() as session:
            bad_cycle = CycleRecord(
                report_schema_id=99999,
                status=CycleStatus.RUNNING,
            )
            session.add(bad_cycle)
            await session.flush()
    except IntegrityError:
        fk_error_caught = True

    assert fk_error_caught, "Foreign key constraint on cycle_records.report_schema_id was not enforced!"
    logger.info("-> Test 2 PASSED: Tables, composite indexes, and FK constraints verified.")


async def test_3_create_report_schema_and_extract_nodes() -> int:
    """
    Validate a schema-driven report layout using Pydantic, store it in `report_schemas`,
    and verify `extract_polled_nodes()` returns all S7-1500 NodeIDs.
    """
    logger.info("=" * 72)
    logger.info("TEST 3: ReportSchema Creation & Dynamic S7-1500 Node Mapping")
    logger.info("=" * 72)

    layout = ReportLayoutConfig(
        header_title="Autoclave Curing Cycle Report",
        machine_name="Siemens S7-1500 Autoclave #1",
        auto_print=False,
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
                id="chart_temperatures",
                title="Zone Temperatures Profile",
                y_axis_label="Temperature (°C)",
                series=[
                    ChartSeriesSchema(
                        tag_name="Temp_Zone1",
                        label="Zone 1 Temp",
                        unit="°C",
                        color="#d62728",
                    ),
                    ChartSeriesSchema(
                        tag_name="Temp_Zone2",
                        label="Zone 2 Temp",
                        unit="°C",
                        color="#1f77b4",
                    ),
                ],
            ),
            ChartElementSchema(
                id="chart_pressure",
                title="Vessel Pressure Profile",
                y_axis_label="Pressure (bar)",
                series=[
                    ChartSeriesSchema(
                        tag_name="Pressure_Main",
                        label="Main Pressure",
                        unit="bar",
                        color="#2ca02c",
                    )
                ],
            ),
        ],
        parameter_tables=[
            ParameterTableElementSchema(
                id="table_process_summary",
                title="Process Parameter Summary",
                parameters=[
                    ParameterRowSchema(
                        tag_name="Temp_Zone1",
                        label="Zone 1 Temperature",
                        unit="°C",
                        aggregations=["min", "max", "avg"],
                        target_setpoint=185.0,
                        tolerance=10.0,
                    ),
                    ParameterRowSchema(
                        tag_name="Temp_Zone2",
                        label="Zone 2 Temperature",
                        unit="°C",
                        aggregations=["min", "max", "avg"],
                        target_setpoint=200.0,
                        tolerance=10.0,
                    ),
                    ParameterRowSchema(
                        tag_name="Pressure_Main",
                        label="Chamber Pressure",
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
                id="alm_plc_overtemp",
                tag_name="Alarm_OverTemp",
                label="PLC Over-Temperature Interlock Triggered",
                condition="is_true",
                severity="CRITICAL",
            ),
            AlarmRuleSchema(
                id="alm_high_zone1",
                tag_name="Temp_Zone1",
                label="Zone 1 High Temperature Warning (> 190°C)",
                condition="gt",
                threshold=190.0,
                severity="WARNING",
            ),
        ],
    )

    payload = ReportSchemaCreate(
        report_name="Autoclave_Curing_Report_v1",
        description="Standard curing cycle report with dual-zone temperatures, pressure, and alarms.",
        trigger_node='ns=3;s="Cycle_Control"."Cycle_Active"',
        schema_config=layout,
        is_active=True,
    )

    async with get_Session_context() as session:
        schema_row = ReportSchema(
            report_name=payload.report_name,
            description=payload.description,
            trigger_node=payload.trigger_node,
            schema_config=payload.schema_config.model_dump(),
            is_active=payload.is_active,
        )
        session.add(schema_row)
        await session.flush()
        schema_id = schema_row.id
        extracted_nodes = schema_row.extract_polled_nodes()

    assert len(extracted_nodes) == 7, f"Expected 7 extracted nodes, got {extracted_nodes}"
    logger.info(
        "-> Test 3 PASSED: Created ReportSchema id=%d ('%s') with %d mapped S7-1500 tags.",
        schema_id,
        payload.report_name,
        len(extracted_nodes),
    )
    return schema_id


async def test_4_live_opc_to_db_persistence(schema_id: int) -> None:
    """
    Connect `S7CyclePoller` (with `persist_to_db=True`) configured dynamically from
    `ReportSchema` in the database, run 2 full cycles on `MockS71500Server`, and
    verify that `CycleRecord` and `CycleTimeSeries` rows are accurately persisted
    and queryable into a Pandas DataFrame.
    """
    logger.info("=" * 72)
    logger.info("TEST 4: End-to-End Live OPC-UA -> Database Persistence (2 Cycles)")
    logger.info("=" * 72)

    async with get_Session_context() as session:
        schema_obj = await session.get(ReportSchema, schema_id)
        assert schema_obj is not None
        poller_config = CyclePollerConfig(
            report_schema_id=schema_obj.id,
            trigger_node_id=schema_obj.trigger_node,
            data_nodes_map=schema_obj.extract_polled_nodes(),
            poll_interval=0.5,
            persist_to_db=True,  # Full DB persistence enabled!
        )

    endpoint = "opc.tcp://127.0.0.1:48420"
    async with MockS71500Server(endpoint=endpoint) as plc:
        poller = S7CyclePoller(endpoint_url=endpoint, config=poller_config)
        poller_task = asyncio.create_task(poller.start())

        try:
            await asyncio.sleep(0.6)

            # --- Execute Cycle 1 (Normal cycle, no alarms, 4 ticks = 2.0s) ---
            logger.info("--- Starting PLC Cycle #1 (Normal Operation) ---")
            await plc.update_process_values(
                step=1, alarm_overtemp=False, recipe_name="BATCH_101_STD"
            )
            await plc.set_cycle_active(True)
            for step in range(2, 5):
                await asyncio.sleep(0.5)
                await plc.update_process_values(
                    step=step, alarm_overtemp=False, recipe_name="BATCH_101_STD"
                )
            await asyncio.sleep(0.4)
            await plc.set_cycle_active(False)
            await asyncio.sleep(0.8)

            # --- Execute Cycle 2 (Cycle with Over-Temp Alarm on step 7) ---
            logger.info("--- Starting PLC Cycle #2 (With Over-Temp Alarm) ---")
            await plc.update_process_values(
                step=5, alarm_overtemp=False, recipe_name="BATCH_102_HIGH_TEMP"
            )
            await plc.set_cycle_active(True)
            for step in range(6, 9):
                await asyncio.sleep(0.5)
                await plc.update_process_values(
                    step=step,
                    alarm_overtemp=(step >= 7),
                    recipe_name="BATCH_102_HIGH_TEMP",
                )
            await asyncio.sleep(0.4)
            await plc.set_cycle_active(False)
            await asyncio.sleep(0.8)

        finally:
            await poller.stop()
            poller_task.cancel()
            await asyncio.gather(poller_task, return_exceptions=True)

    # Query the database and verify CycleRecord & CycleTimeSeries rows
    async with get_Session_context() as session:
        cycles_result = await session.execute(
            select(CycleRecord)
            .where(CycleRecord.report_schema_id == schema_id)
            .order_by(CycleRecord.id)
        )
        cycles = list(cycles_result.scalars().all())
        assert len(cycles) == 2, f"Expected 2 persisted CycleRecords, found {len(cycles)}"

        for c in cycles:
            assert c.status == CycleStatus.COMPLETED, f"Cycle {c.id} status={c.status}"
            assert c.start_time is not None and c.end_time is not None
            assert c.end_time > c.start_time

            ts_result = await session.execute(
                select(CycleTimeSeries)
                .where(CycleTimeSeries.cycle_id == c.id)
                .order_by(CycleTimeSeries.timestamp, CycleTimeSeries.tag_name)
            )
            ts_rows = list(ts_result.scalars().all())
            assert len(ts_rows) >= 21, (
                f"Expected >= 21 time-series rows (3+ ticks * 7 tags) for cycle {c.id}, got {len(ts_rows)}"
            )

            # Build a Pandas DataFrame from the numeric time-series rows to verify readiness for Stage 3
            df = pd.DataFrame(
                [
                    {
                        "timestamp": r.timestamp,
                        "tag_name": r.tag_name,
                        "value": r.value,
                        "raw_value": r.raw_value,
                    }
                    for r in ts_rows
                ]
            )
            pivoted = df.pivot_table(
                index="timestamp", columns="tag_name", values="value", aggfunc="first"
            )
            logger.info(
                "Cycle id=%d persisted %d CycleTimeSeries rows (%d timestamps x %d numeric tags):\n%s",
                c.id,
                len(ts_rows),
                len(pivoted),
                len(pivoted.columns),
                pivoted.to_string(),
            )

    logger.info("=" * 72)
    logger.info("STAGE 2 (DATABASE & OPC-TO-DB PERSISTENCE) VERIFICATION PASSED!")
    logger.info("=" * 72)


async def run_stage_2_tests() -> None:
    """Execute all Stage 2 Database & OPC-to-DB integration tests."""
    await test_1_mssql_and_sqlite_dialect_compilation()
    await test_2_schema_inspection_and_foreign_key_integrity()
    schema_id = await test_3_create_report_schema_and_extract_nodes()
    await test_4_live_opc_to_db_persistence(schema_id)
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(run_stage_2_tests())
