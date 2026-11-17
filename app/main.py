"""
FastAPI Prototype Application for the Siemens S7-1500 OPC-UA Cycle Reporting System.

Integrates:
1. Simple Web Dashboard (`GET /`) & Report Designer REST API (`/api/schemas`)
2. Non-Redundant OPC-UA Polling (`header_tags` read ONCE on cycle start for Batch_Number;
   dynamic `tags` bulk-polled every 0.5s)
3. Watchdog CSV Alarm Folder Monitor (`app/csv_watcher.py`) moving processed CSVs to `.bckup/`
4. Automated Pandas + Matplotlib + Jinja2 + WeasyPrint PDF Report Generation
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import logging
import os
from pathlib import Path
from typing import Any, AsyncGenerator, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.csv_watcher import DEFAULT_WATCH_DIR, AlarmCSVWatcherService
from app.database import get_Session_context, get_db_session, init_db
from app.models import (
    CycleAlarmLog,
    CycleRecord,
    CycleStatus,
    CycleTimeSeries,
    ReportSchema,
)
from app.opc_client import CyclePollerConfig, S7CyclePoller
from app.report_generator import TEMPLATES_DIR, generate_cycle_pdf_report
from app.schemas import (
    ChartElementSchema,
    ChartSeriesSchema,
    CycleAlarmLogRead,
    CycleRecordRead,
    ParameterRowSchema,
    ParameterTableElementSchema,
    ReportLayoutConfig,
    ReportSchemaCreate,
    ReportSchemaRead,
)

logger = logging.getLogger(__name__)

# Prototype runtime state
_csv_watcher: Optional[AlarmCSVWatcherService] = None
_poller: Optional[S7CyclePoller] = None
_poller_task: Optional[asyncio.Task[Any]] = None
_mock_plc: Optional[Any] = None


async def ensure_default_report_schema() -> ReportSchema:
    """
    Seed a clean default ReportSchema if the database is empty.
    Demonstrates separating `header_tags` (read ONCE at cycle start, e.g. Batch_Number)
    from `tags` (polled every 0.5s during an active cycle).
    """
    async with get_Session_context() as session:
        res = await session.execute(select(ReportSchema).order_by(ReportSchema.id).limit(1))
        existing = res.scalar_one_or_none()
        if existing is not None:
            return existing

        default_layout = ReportLayoutConfig(
            header_title="Siemens S7-1500 Machine Cycle Report",
            machine_name="S7-1500 Line #1",
            auto_print=True,
            printer_name="Factory_Default_Printer",
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
                    id="chart_temps",
                    title="Zone 1 & Zone 2 Temperatures (°C)",
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
                    id="chart_pressure",
                    title="Chamber Pressure Profile (bar)",
                    y_axis_label="Pressure (bar)",
                    series=[
                        ChartSeriesSchema(
                            tag_name="Pressure_Main",
                            label="Main Pressure",
                            unit="bar",
                            color="#16a34a",
                        )
                    ],
                ),
            ],
            parameter_tables=[
                ParameterTableElementSchema(
                    id="tbl_summary",
                    title="Process Parameter Summary",
                    parameters=[
                        ParameterRowSchema(
                            tag_name="Temp_Zone1",
                            label="Zone 1 Temperature",
                            unit="°C",
                            target_setpoint=185.0,
                            tolerance=8.0,
                        ),
                        ParameterRowSchema(
                            tag_name="Temp_Zone2",
                            label="Zone 2 Temperature",
                            unit="°C",
                            target_setpoint=198.0,
                            tolerance=8.0,
                        ),
                        ParameterRowSchema(
                            tag_name="Pressure_Main",
                            label="Chamber Pressure",
                            unit="bar",
                            target_setpoint=4.5,
                            tolerance=0.5,
                        ),
                        ParameterRowSchema(
                            tag_name="Motor_Speed_RPM",
                            label="Fan Speed",
                            unit="RPM",
                        ),
                    ],
                )
            ],
            alarms=[],
        )

        schema_row = ReportSchema(
            report_name="Default_S7_Cycle_Report",
            description="Prototype schema with one-shot Batch_Number header read & 0.5s bulk polling.",
            trigger_node='ns=3;s="Cycle_Control"."Cycle_Active"',
            schema_config=default_layout.model_dump(),
            is_active=True,
        )
        session.add(schema_row)
        await session.flush()
        return schema_row


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Start database, default schema, Watchdog CSV monitor, and prototype OPC-UA poller.
    """
    global _csv_watcher, _poller, _poller_task, _mock_plc

    await init_db()
    default_schema = await ensure_default_report_schema()

    # Start Watchdog CSV watcher on ./watched_alarms
    _csv_watcher = AlarmCSVWatcherService(
        watch_dir=DEFAULT_WATCH_DIR,
        active_cycle_id_provider=lambda: _poller.active_cycle_id if _poller else None,
    )
    await _csv_watcher.start()

    # Start embedded Mock S7-1500 server + S7CyclePoller unless disabled
    if os.getenv("DISABLE_BACKGROUND_POLLER", "false").lower() != "true":
        from tests.mock_s7_server import MockS71500Server

        endpoint = os.getenv("OPCUA_ENDPOINT_URL", "opc.tcp://127.0.0.1:48440")
        if endpoint.startswith("opc.tcp://127.0.0.1"):
            _mock_plc = MockS71500Server(endpoint=endpoint)
            await _mock_plc.__aenter__()

        poller_cfg = CyclePollerConfig(
            report_schema_id=default_schema.id,
            trigger_node_id=default_schema.trigger_node,
            header_nodes_map=default_schema.extract_header_nodes(),
            data_nodes_map=default_schema.extract_polled_nodes(),
            poll_interval=0.5,
            persist_to_db=True,
            auto_generate_pdf=True,
        )
        _poller = S7CyclePoller(endpoint_url=endpoint, config=poller_cfg)
        _poller_task = asyncio.create_task(_poller.start(), name="prototype-opc-poller")

    try:
        yield
    finally:
        if _poller is not None:
            await _poller.stop()
        if _poller_task is not None:
            _poller_task.cancel()
            await asyncio.gather(_poller_task, return_exceptions=True)
        if _mock_plc is not None:
            await _mock_plc.__aexit__(None, None, None)
        if _csv_watcher is not None:
            await _csv_watcher.stop()


app = FastAPI(
    title="Siemens S7-1500 OPC-UA Cycle Reporting Prototype",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def prototype_dashboard() -> HTMLResponse:
    """Serve the simple single-page Prototype Dashboard UI."""
    html_path = TEMPLATES_DIR / "dashboard.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.get("/health", tags=["System"])
async def health_check() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok", "service": "opc-ua-report-generator"}


# =========================================================================
# Prototype Control & Status Endpoints
# =========================================================================


class SimulateCycleRequest(BaseModel):
    batch_number: str = "BATCH-2026-101"
    recipe_name: str = "RECIPE_STD_200C"
    drop_csv_alarm: bool = False


@app.get("/api/prototype/status", tags=["Prototype"])
async def get_prototype_status() -> dict[str, Any]:
    """Return live OPC-UA poller state and Watchdog CSV `.bckup` status."""
    watch_dir = _csv_watcher.watch_dir if _csv_watcher else DEFAULT_WATCH_DIR.resolve()
    bckup_dir = watch_dir / ".bckup"
    backup_files = (
        sorted(p.name for p in bckup_dir.glob("*.csv"))
        if bckup_dir.is_dir()
        else []
    )
    return {
        "poller_state": _poller.state.value if _poller else "STOPPED",
        "active_cycle_id": _poller.active_cycle_id if _poller else None,
        "watch_dir": str(watch_dir),
        "backup_files": backup_files,
        "processed_csv_files_count": _csv_watcher.processed_files_count if _csv_watcher else 0,
        "ingested_alarms_count": _csv_watcher.ingested_alarms_count if _csv_watcher else 0,
    }


@app.post("/api/prototype/drop-csv", tags=["Prototype"])
async def drop_sample_csv_alarm() -> dict[str, Any]:
    """Drop a sample alarm CSV file into the watched folder to trigger Watchdog ingestion."""
    watch_dir = _csv_watcher.watch_dir if _csv_watcher else DEFAULT_WATCH_DIR.resolve()
    watch_dir.mkdir(parents=True, exist_ok=True)

    now_utc = datetime.now(timezone.utc)
    filename = f"plc_alarms_{now_utc.strftime('%H%M%S_%f')[:10]}.csv"
    csv_path = watch_dir / filename

    csv_content = (
        "timestamp,alarm_code,alarm_text,severity\n"
        f"{now_utc.isoformat()},ALM_201,Zone 1 Heater Current High Deviation,WARNING\n"
        f"{now_utc.isoformat()},ALM_305,Chamber Pressure Relief Valve Check,CRITICAL\n"
    )
    csv_path.write_text(csv_content, encoding="utf-8")
    return {"status": "dropped", "filename": filename, "path": str(csv_path)}


@app.post("/api/prototype/simulate-cycle", tags=["Prototype"])
async def trigger_simulated_plc_cycle(req: SimulateCycleRequest) -> dict[str, Any]:
    """
    Simulate a 2.0-second machine cycle on the embedded S7-1500 mock PLC:
    1. Sets `Batch_Number` and `Recipe_Name` on the PLC before rising edge.
    2. Sets `Cycle_Active = True` (poller reads `Batch_Number` ONCE, then polls 0.5s data).
    3. Optionally drops a CSV alarm file mid-cycle into `watched_alarms/`.
    4. Sets `Cycle_Active = False` -> triggers automatic PDF generation.
    """
    if _mock_plc is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Embedded Mock S7-1500 server is not active.",
        )

    async def _run_cycle() -> None:
        await _mock_plc.update_process_values(
            step=1,
            recipe_name=req.recipe_name,
            batch_number=req.batch_number,
        )
        await _mock_plc.set_cycle_active(True)
        for step in range(2, 6):
            await asyncio.sleep(0.5)
            await _mock_plc.update_process_values(
                step=step,
                recipe_name=req.recipe_name,
                batch_number=req.batch_number,
            )
            if step == 3 and req.drop_csv_alarm:
                await drop_sample_csv_alarm()
        await _mock_plc.set_cycle_active(False)

    asyncio.create_task(_run_cycle())
    return {
        "status": "cycle_started",
        "batch_number": req.batch_number,
        "drop_csv_alarm": req.drop_csv_alarm,
    }


@app.get(
    "/api/alarms",
    response_model=list[CycleAlarmLogRead],
    tags=["Cycles & Reports"],
)
async def list_ingested_csv_alarms(
    cycle_id: Optional[int] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    db: AsyncSession = Depends(get_db_session),
) -> list[CycleAlarmLog]:
    """List alarm records consumed from watched CSV files."""
    stmt = select(CycleAlarmLog).order_by(CycleAlarmLog.id.desc()).limit(limit)
    if cycle_id is not None:
        stmt = stmt.where(CycleAlarmLog.cycle_id == cycle_id)
    res = await db.execute(stmt)
    return list(res.scalars().all())


# =========================================================================
# Report Designer Endpoints (ReportSchema CRUD)
# =========================================================================


@app.post(
    "/api/schemas",
    response_model=ReportSchemaRead,
    status_code=status.HTTP_201_CREATED,
    tags=["Report Designer"],
)
async def create_report_schema(
    payload: ReportSchemaCreate,
    db: AsyncSession = Depends(get_db_session),
) -> ReportSchema:
    """Create a new dynamic JSON ReportSchema mapping S7-1500 NodeIDs to visual elements."""
    new_schema = ReportSchema(
        report_name=payload.report_name,
        description=payload.description,
        trigger_node=payload.trigger_node,
        schema_config=payload.schema_config.model_dump(),
        is_active=payload.is_active,
    )
    db.add(new_schema)
    try:
        await db.commit()
        await db.refresh(new_schema)
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"ReportSchema with name '{payload.report_name}' already exists.",
        ) from exc
    return new_schema


@app.get(
    "/api/schemas",
    response_model=list[ReportSchemaRead],
    tags=["Report Designer"],
)
async def list_report_schemas(
    active_only: bool = Query(default=False),
    db: AsyncSession = Depends(get_db_session),
) -> list[ReportSchema]:
    """List all configured report schemas."""
    stmt = select(ReportSchema).order_by(ReportSchema.id)
    if active_only:
        stmt = stmt.where(ReportSchema.is_active.is_(True))
    result = await db.execute(stmt)
    return list(result.scalars().all())


@app.get(
    "/api/schemas/{schema_id}",
    response_model=ReportSchemaRead,
    tags=["Report Designer"],
)
async def get_report_schema(
    schema_id: int,
    db: AsyncSession = Depends(get_db_session),
) -> ReportSchema:
    """Get a single ReportSchema by ID, including its JSON layout configuration."""
    schema_obj = await db.get(ReportSchema, schema_id)
    if schema_obj is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"ReportSchema id={schema_id} not found.",
        )
    return schema_obj


@app.put(
    "/api/schemas/{schema_id}",
    response_model=ReportSchemaRead,
    tags=["Report Designer"],
)
async def update_report_schema(
    schema_id: int,
    payload: ReportSchemaCreate,
    db: AsyncSession = Depends(get_db_session),
) -> ReportSchema:
    """Update an existing ReportSchema layout configuration."""
    schema_obj = await db.get(ReportSchema, schema_id)
    if schema_obj is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"ReportSchema id={schema_id} not found.",
        )

    schema_obj.report_name = payload.report_name
    schema_obj.description = payload.description
    schema_obj.trigger_node = payload.trigger_node
    schema_obj.schema_config = payload.schema_config.model_dump()
    schema_obj.is_active = payload.is_active

    try:
        await db.commit()
        await db.refresh(schema_obj)
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Could not update ReportSchema: {exc}",
        ) from exc
    return schema_obj


@app.delete(
    "/api/schemas/{schema_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["Report Designer"],
)
async def delete_report_schema(
    schema_id: int,
    db: AsyncSession = Depends(get_db_session),
) -> None:
    """Delete a ReportSchema (fails with 409 if CycleRecords reference it)."""
    schema_obj = await db.get(ReportSchema, schema_id)
    if schema_obj is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"ReportSchema id={schema_id} not found.",
        )
    await db.delete(schema_obj)
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Cannot delete ReportSchema because historical CycleRecords reference it.",
        ) from exc


# =========================================================================
# Cycle Records & PDF Report Endpoints
# =========================================================================


@app.get(
    "/api/cycles",
    response_model=list[CycleRecordRead],
    tags=["Cycles & Reports"],
)
async def list_cycles(
    report_schema_id: Optional[int] = Query(default=None),
    cycle_status: Optional[CycleStatus] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    db: AsyncSession = Depends(get_db_session),
) -> list[CycleRecord]:
    """List machine cycle records ordered by most recent first."""
    stmt = select(CycleRecord).order_by(CycleRecord.id.desc()).limit(limit)
    if report_schema_id is not None:
        stmt = stmt.where(CycleRecord.report_schema_id == report_schema_id)
    if cycle_status is not None:
        stmt = stmt.where(CycleRecord.status == cycle_status)
    result = await db.execute(stmt)
    return list(result.scalars().all())


@app.get(
    "/api/cycles/{cycle_id}",
    tags=["Cycles & Reports"],
)
async def get_cycle_details(
    cycle_id: int,
    db: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    """Get a cycle record along with the count of logged 0.5s time-series rows and CSV alarms."""
    cycle = await db.get(CycleRecord, cycle_id)
    if cycle is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"CycleRecord id={cycle_id} not found.",
        )
    count_res = await db.execute(
        select(func.count(CycleTimeSeries.id)).where(
            CycleTimeSeries.cycle_id == cycle_id
        )
    )
    total_points = int(count_res.scalar_one() or 0)

    alarms_res = await db.execute(
        select(CycleAlarmLog)
        .where(CycleAlarmLog.cycle_id == cycle_id)
        .order_by(CycleAlarmLog.timestamp)
    )
    csv_alarms = [
        CycleAlarmLogRead.model_validate(a).model_dump()
        for a in alarms_res.scalars().all()
    ]
    return {
        "cycle": CycleRecordRead.model_validate(cycle).model_dump(),
        "timeseries_points_count": total_points,
        "csv_alarms": csv_alarms,
    }


@app.post(
    "/api/cycles/{cycle_id}/generate-report",
    tags=["Cycles & Reports"],
)
async def trigger_cycle_report_endpoint(
    cycle_id: int,
    force_print: Optional[bool] = Query(default=None),
    db: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    """Manually trigger or re-generate a PDF report for a cycle."""
    cycle = await db.get(CycleRecord, cycle_id)
    if cycle is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"CycleRecord id={cycle_id} not found.",
        )

    result = await generate_cycle_pdf_report(
        cycle_id=cycle_id,
        force_print=force_print,
    )
    return {
        "cycle_id": result.cycle_id,
        "status": result.status.value,
        "pdf_file_path": str(result.pdf_path),
        "triggered_alarms_count": result.triggered_alarms_count,
        "printed": bool(result.print_result and result.print_result.success),
    }


@app.get(
    "/api/cycles/{cycle_id}/pdf",
    response_class=FileResponse,
    tags=["Cycles & Reports"],
)
async def download_cycle_pdf(
    cycle_id: int,
    db: AsyncSession = Depends(get_db_session),
) -> FileResponse:
    """Download the generated PDF report for a given cycle."""
    cycle = await db.get(CycleRecord, cycle_id)
    if cycle is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"CycleRecord id={cycle_id} not found.",
        )
    if not cycle.pdf_file_path or not Path(cycle.pdf_file_path).is_file():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"PDF report has not been generated yet for cycle_id={cycle_id}.",
        )
    pdf_path = Path(cycle.pdf_file_path)
    return FileResponse(
        path=str(pdf_path),
        media_type="application/pdf",
        filename=pdf_path.name,
    )
