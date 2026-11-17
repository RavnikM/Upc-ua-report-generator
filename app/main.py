"""
FastAPI Backend for the Siemens S7-1500 OPC-UA Cycle Reporting Application.

Ready for testing against a real S7-1500 PLC (or optional local mock PLC):
1. Live PLC Connection Configuration & Hot-Reloading (`/api/config`, `/api/poller/*`)
2. Live PLC Diagnostics: Connection Probe, Bulk Node Value/StatusCode Tester,
   and OPC-UA Address Space Browser (`/api/plc/*`)
3. Visual Data Mapping & Report Schema Editor (`/api/schemas`)
4. Watchdog CSV Alarm Folder Monitor (`app/csv_watcher.py`) moving processed CSVs to `.bckup/`
5. Automated Pandas + Matplotlib + Jinja2 + WeasyPrint PDF Report Generation
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
    SystemConfig,
)
from app.opc_client import (
    CyclePollerConfig,
    S7CyclePoller,
    browse_plc_node,
    probe_plc_connection,
    test_read_plc_nodes,
)
from app.report_generator import TEMPLATES_DIR, generate_cycle_pdf_report
from app.schemas import (
    ChartElementSchema,
    ChartSeriesSchema,
    CycleAlarmLogRead,
    CycleRecordRead,
    PLCConnectionTestRequest,
    PLCNodeTestRequest,
    ParameterRowSchema,
    ParameterTableElementSchema,
    ReportLayoutConfig,
    ReportSchemaCreate,
    ReportSchemaRead,
    SystemConfigRead,
    SystemConfigUpdate,
)

logger = logging.getLogger(__name__)

# Runtime background service references
MOCK_PLC_ENDPOINT = "opc.tcp://127.0.0.1:48440"
_csv_watcher: Optional[AlarmCSVWatcherService] = None
_poller: Optional[S7CyclePoller] = None
_poller_task: Optional[asyncio.Task[Any]] = None
_mock_plc: Optional[Any] = None
_service_lock = asyncio.Lock()


async def ensure_mock_plc_running() -> str:
    """
    Start the embedded MockS71500Server if it is not already running.
    If port 48440 is already bound by an active MockS71500Server process,
    reuse `MOCK_PLC_ENDPOINT` immediately without attempting to re-bind.
    """
    global _mock_plc
    if _mock_plc is not None:
        return _mock_plc.endpoint

    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.1)
        if sock.connect_ex(("127.0.0.1", 48440)) == 0:
            return MOCK_PLC_ENDPOINT

    from tests.mock_s7_server import MockS71500Server

    candidate = MockS71500Server(endpoint=MOCK_PLC_ENDPOINT)
    try:
        await candidate.__aenter__()
        _mock_plc = candidate
        return _mock_plc.endpoint
    except OSError:
        return MOCK_PLC_ENDPOINT


async def stop_mock_plc_running() -> None:
    """Stop the embedded MockS71500Server if running."""
    global _mock_plc
    if _mock_plc is not None:
        await _mock_plc.__aexit__(None, None, None)
        _mock_plc = None


async def ensure_default_report_schema() -> ReportSchema:
    """
    Seed a default ReportSchema if the database is empty.
    Separates `header_tags` (read ONCE at cycle start, e.g. Batch_Number)
    from `tags` (polled every 0.5s during an active cycle).
    """
    async with get_Session_context() as session:
        res = await session.execute(
            select(ReportSchema).order_by(ReportSchema.id).limit(1)
        )
        existing = res.scalar_one_or_none()
        if existing is not None:
            return existing

        default_layout = ReportLayoutConfig(
            header_title="Siemens S7-1500 Machine Cycle Report",
            machine_name="S7-1500 Line #1",
            auto_print=False,
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
            description="S7-1500 schema with one-shot Batch_Number header read & 0.5s bulk polling.",
            trigger_node='ns=3;s="Cycle_Control"."Cycle_Active"',
            schema_config=default_layout.model_dump(),
            is_active=True,
        )
        session.add(schema_row)
        await session.flush()
        return schema_row


async def ensure_system_config(default_schema_id: int) -> SystemConfig:
    """Load or create the singleton `SystemConfig` row (id=1)."""
    async with get_Session_context() as session:
        res = await session.execute(
            select(SystemConfig).order_by(SystemConfig.id).limit(1)
        )
        cfg = res.scalar_one_or_none()
        if cfg is not None:
            if cfg.active_schema_id is None:
                cfg.active_schema_id = default_schema_id
            return cfg

        env_endpoint = os.getenv("OPCUA_ENDPOINT_URL", "opc.tcp://192.168.0.1:4840")
        env_interval = float(os.getenv("OPCUA_POLL_INTERVAL_SECONDS", "0.5"))
        env_watch_dir = os.getenv("ALARM_CSV_WATCH_DIR", str(DEFAULT_WATCH_DIR))
        env_mock = os.getenv("USE_MOCK_PLC", "true").lower() == "true"

        cfg = SystemConfig(
            opcua_endpoint_url=env_endpoint,
            opcua_username=os.getenv("OPCUA_USERNAME") or None,
            opcua_password=os.getenv("OPCUA_PASSWORD") or None,
            poll_interval_seconds=env_interval,
            csv_watch_dir=env_watch_dir,
            use_mock_plc=env_mock,
            active_schema_id=default_schema_id,
        )
        session.add(cfg)
        await session.flush()
        return cfg


async def stop_poller_service(stop_mock: bool = False) -> None:
    """Cleanly stop the running `S7CyclePoller` (and optionally the embedded mock PLC)."""
    global _poller, _poller_task
    if _poller is not None:
        await _poller.stop()
        _poller = None
    if _poller_task is not None:
        _poller_task.cancel()
        await asyncio.gather(_poller_task, return_exceptions=True)
        _poller_task = None
    if stop_mock:
        await stop_mock_plc_running()


async def restart_runtime_services(start_poller: bool = True) -> dict[str, Any]:
    """
    Reload `SystemConfig` and the active `ReportSchema` from the database,
    reconfigure `AlarmCSVWatcherService`, and start/restart `S7CyclePoller`.
    """
    global _csv_watcher, _poller, _poller_task

    async with _service_lock:
        await stop_poller_service(stop_mock=False)

        async with get_Session_context() as session:
            cfg_res = await session.execute(
                select(SystemConfig).order_by(SystemConfig.id).limit(1)
            )
            sys_cfg = cfg_res.scalar_one()

            schema_obj: Optional[ReportSchema] = None
            if sys_cfg.active_schema_id:
                schema_obj = await session.get(ReportSchema, sys_cfg.active_schema_id)
            if schema_obj is None:
                s_res = await session.execute(
                    select(ReportSchema).order_by(ReportSchema.id).limit(1)
                )
                schema_obj = s_res.scalar_one()
                sys_cfg.active_schema_id = schema_obj.id

            endpoint_url = sys_cfg.opcua_endpoint_url
            username = sys_cfg.opcua_username
            password = sys_cfg.opcua_password
            poll_interval = sys_cfg.poll_interval_seconds
            watch_dir_str = sys_cfg.csv_watch_dir
            use_mock = sys_cfg.use_mock_plc

            schema_id = schema_obj.id
            trigger_node = schema_obj.trigger_node
            header_nodes = schema_obj.extract_header_nodes()
            polled_nodes = schema_obj.extract_polled_nodes()

        # Reconfigure or start CSV Watcher
        if _csv_watcher is not None:
            await _csv_watcher.stop()
        _csv_watcher = AlarmCSVWatcherService(
            watch_dir=watch_dir_str,
            active_cycle_id_provider=lambda: _poller.active_cycle_id if _poller else None,
        )
        await _csv_watcher.start()

        # Manage embedded MockS71500Server lifecycle based on use_mock flag
        if use_mock:
            endpoint_url = await ensure_mock_plc_running()
            username = None
            password = None
        else:
            await stop_mock_plc_running()

        if not start_poller:
            return {"poller_running": False, "endpoint_url": endpoint_url}

        poller_cfg = CyclePollerConfig(
            report_schema_id=schema_id,
            trigger_node_id=trigger_node,
            header_nodes_map=header_nodes,
            data_nodes_map=polled_nodes,
            poll_interval=poll_interval,
            persist_to_db=True,
            auto_generate_pdf=True,
        )
        _poller = S7CyclePoller(
            endpoint_url=endpoint_url,
            config=poller_cfg,
            username=username,
            password=password,
        )
        _poller_task = asyncio.create_task(
            _poller.start(), name="s7-opc-poller-task"
        )
        return {
            "poller_running": True,
            "endpoint_url": endpoint_url,
            "use_mock_plc": use_mock,
            "active_schema_id": schema_id,
        }


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
    """Initialize database, default schema, system config, and background services."""
    await init_db()
    default_schema = await ensure_default_report_schema()
    await ensure_system_config(default_schema.id)

    auto_start = os.getenv("DISABLE_BACKGROUND_POLLER", "false").lower() != "true"
    await restart_runtime_services(start_poller=auto_start)

    try:
        yield
    finally:
        await stop_poller_service(stop_mock=True)
        if _csv_watcher is not None:
            await _csv_watcher.stop()


app = FastAPI(
    title="Siemens S7-1500 OPC-UA Cycle Reporting System",
    version="1.1.0",
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
async def ui_dashboard() -> HTMLResponse:
    """Serve the Web UI for PLC Configuration, Tag Mapping, and Report Editing."""
    html_path = TEMPLATES_DIR / "dashboard.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.get("/health", tags=["System"])
async def health_check() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok", "service": "opc-ua-report-generator"}


# =========================================================================
# System Configuration & Live PLC Diagnostics Endpoints
# =========================================================================


@app.get(
    "/api/config",
    response_model=SystemConfigRead,
    tags=["PLC & System Config"],
)
async def get_system_config(
    db: AsyncSession = Depends(get_db_session),
) -> SystemConfig:
    """Retrieve current PLC endpoint, polling, and CSV watch folder settings."""
    res = await db.execute(select(SystemConfig).order_by(SystemConfig.id).limit(1))
    cfg = res.scalar_one_or_none()
    if cfg is None:
        default_schema = await ensure_default_report_schema()
        return await ensure_system_config(default_schema.id)
    return cfg


@app.put(
    "/api/config",
    response_model=SystemConfigRead,
    tags=["PLC & System Config"],
)
async def update_system_config(
    payload: SystemConfigUpdate,
    restart_poller: bool = Query(default=True),
    db: AsyncSession = Depends(get_db_session),
) -> SystemConfig:
    """
    Update PLC connection settings (`opcua_endpoint_url`, credentials, `poll_interval_seconds`,
    `csv_watch_dir`, `use_mock_plc`, `active_schema_id`) and hot-reload the poller.
    """
    res = await db.execute(select(SystemConfig).order_by(SystemConfig.id).limit(1))
    cfg = res.scalar_one()

    cfg.opcua_endpoint_url = payload.opcua_endpoint_url.strip()
    cfg.opcua_username = (
        payload.opcua_username.strip() if payload.opcua_username else None
    )
    cfg.opcua_password = (
        payload.opcua_password if payload.opcua_password else None
    )
    cfg.poll_interval_seconds = payload.poll_interval_seconds
    cfg.csv_watch_dir = payload.csv_watch_dir.strip()
    cfg.use_mock_plc = payload.use_mock_plc
    if payload.active_schema_id is not None:
        cfg.active_schema_id = payload.active_schema_id

    await db.commit()
    await db.refresh(cfg)

    if restart_poller:
        await restart_runtime_services(start_poller=True)

    return cfg


@app.post("/api/poller/restart", tags=["PLC & System Config"])
async def api_restart_poller() -> dict[str, Any]:
    """Start or restart the OPC-UA poller with current DB configuration."""
    return await restart_runtime_services(start_poller=True)


@app.post("/api/poller/stop", tags=["PLC & System Config"])
async def api_stop_poller() -> dict[str, Any]:
    """Stop the background OPC-UA poller."""
    async with _service_lock:
        await stop_poller_service()
    return {"poller_running": False, "poller_state": "STOPPED"}


async def _resolve_plc_connection_params(
    db: AsyncSession,
    override_url: Optional[str] = None,
    override_user: Optional[str] = None,
    override_pass: Optional[str] = None,
    override_use_mock: Optional[bool] = None,
) -> tuple[str, Optional[str], Optional[str]]:
    """
    Resolve effective PLC connection parameters for diagnostic calls.
    If Mock PLC mode is enabled (either in DB config or checked in the UI),
    automatically starts the embedded MockS71500Server and returns its endpoint.
    """
    res = await db.execute(select(SystemConfig).order_by(SystemConfig.id).limit(1))
    cfg = res.scalar_one()

    use_mock = (
        override_use_mock if override_use_mock is not None else cfg.use_mock_plc
    )
    if use_mock:
        mock_ep = await ensure_mock_plc_running()
        return mock_ep, None, None

    if _poller is not None and not override_url:
        url = _poller.endpoint_url
    else:
        url = (override_url or cfg.opcua_endpoint_url).strip()
    user = override_user if override_user is not None else cfg.opcua_username
    pw = override_pass if override_pass is not None else cfg.opcua_password
    return url, user, pw


@app.post("/api/plc/test-connection", tags=["PLC Diagnostics & Browsing"])
async def api_test_plc_connection(
    req: PLCConnectionTestRequest,
    db: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    """
    Test connection to the real (or mock) Siemens S7-1500 PLC OPC-UA server,
    returning round-trip latency and server namespaces.
    """
    url, user, pw = await _resolve_plc_connection_params(
        db,
        req.opcua_endpoint_url,
        req.opcua_username,
        req.opcua_password,
        req.use_mock_plc,
    )
    return await probe_plc_connection(url, user, pw)


@app.post("/api/plc/test-nodes", tags=["PLC Diagnostics & Browsing"])
async def api_test_plc_nodes(
    req: PLCNodeTestRequest,
    db: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    """
    Execute a live bulk read of the specified OPC-UA NodeIds on the PLC and return
    each tag's current value, data type, and OPC-UA StatusCode (`Good` vs `BadNodeIdUnknown`).
    """
    url, user, pw = await _resolve_plc_connection_params(
        db,
        req.opcua_endpoint_url,
        req.opcua_username,
        req.opcua_password,
        req.use_mock_plc,
    )
    try:
        results = await test_read_plc_nodes(url, req.node_ids, user, pw)
        return {"ok": True, "endpoint_url": url, "results": results, "error": None}
    except Exception as exc:
        return {"ok": False, "endpoint_url": url, "results": [], "error": str(exc)}


@app.get("/api/plc/browse", tags=["PLC Diagnostics & Browsing"])
async def api_browse_plc(
    parent_node_id: Optional[str] = Query(default=None),
    use_mock_plc: Optional[bool] = Query(default=None),
    db: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    """
    Browse child nodes of `parent_node_id` (or root `Objects` folder if omitted)
    on the configured S7-1500 PLC.
    """
    url, user, pw = await _resolve_plc_connection_params(
        db, override_use_mock=use_mock_plc
    )
    try:
        data = await browse_plc_node(url, parent_node_id, user, pw)
        return {"ok": True, "endpoint_url": url, **data, "error": None}
    except Exception as exc:
        return {
            "ok": False,
            "endpoint_url": url,
            "parent_node_id": parent_node_id,
            "children": [],
            "error": str(exc),
        }


# =========================================================================
# Runtime Status & Local Simulation Helpers
# =========================================================================


class SimulateCycleRequest(BaseModel):
    batch_number: str = "BATCH-2026-101"
    recipe_name: str = "RECIPE_STD_200C"
    drop_csv_alarm: bool = False


@app.get("/api/prototype/status", tags=["PLC & System Config"])
async def get_prototype_status() -> dict[str, Any]:
    """Return live OPC-UA poller state, connection diagnostics, and Watchdog CSV status."""
    watch_dir = (
        _csv_watcher.watch_dir if _csv_watcher else DEFAULT_WATCH_DIR.resolve()
    )
    bckup_dir = watch_dir / ".bckup"
    backup_files = (
        sorted(p.name for p in bckup_dir.glob("*.csv"))
        if bckup_dir.is_dir()
        else []
    )
    return {
        "poller_running": _poller is not None,
        "poller_state": _poller.state.value if _poller else "STOPPED",
        "is_connected": _poller.is_connected if _poller else False,
        "endpoint_url": _poller.endpoint_url if _poller else None,
        "last_error": _poller.last_error if _poller else None,
        "invalid_data_nodes": _poller.invalid_data_nodes if _poller else {},
        "reconnect_count": _poller.reconnect_count if _poller else 0,
        "active_cycle_id": _poller.active_cycle_id if _poller else None,
        "active_header_data": _poller.active_header_data if _poller else {},
        "mock_plc_active": (
            _mock_plc is not None
            or bool(_poller and _poller.endpoint_url == MOCK_PLC_ENDPOINT)
        ),
        "watch_dir": str(watch_dir),
        "backup_files": backup_files,
        "processed_csv_files_count": (
            _csv_watcher.processed_files_count if _csv_watcher else 0
        ),
        "ingested_alarms_count": (
            _csv_watcher.ingested_alarms_count if _csv_watcher else 0
        ),
    }


@app.post("/api/prototype/drop-csv", tags=["Prototype"])
async def drop_sample_csv_alarm() -> dict[str, Any]:
    """Drop a sample alarm CSV file into the watched folder to test Watchdog ingestion."""
    watch_dir = (
        _csv_watcher.watch_dir if _csv_watcher else DEFAULT_WATCH_DIR.resolve()
    )
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
    """Simulate a machine cycle when running in Mock PLC mode."""
    if _mock_plc is None and not (
        _poller and _poller.endpoint_url == MOCK_PLC_ENDPOINT
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Local Mock PLC is not enabled. Enable 'Use Embedded Mock S7-1500 Server' in Tab 1 to simulate cycles without a physical PLC.",
        )

    async def _run_cycle() -> None:
        if _mock_plc is not None:
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
        else:
            from asyncua import Client, ua

            async with Client(url=MOCK_PLC_ENDPOINT) as c:
                n_batch = c.get_node('ns=3;s="Cycle_Data"."Batch_Number"')
                n_recipe = c.get_node('ns=3;s="Cycle_Data"."Recipe_Name"')
                n_trig = c.get_node('ns=3;s="Cycle_Control"."Cycle_Active"')
                n_t1 = c.get_node('ns=3;s="Cycle_Data"."Temp_Zone1"')
                await n_batch.write_value(
                    ua.Variant(req.batch_number, ua.VariantType.String)
                )
                await n_recipe.write_value(
                    ua.Variant(req.recipe_name, ua.VariantType.String)
                )
                await n_trig.write_value(ua.Variant(True, ua.VariantType.Boolean))
                for step in range(2, 6):
                    await asyncio.sleep(0.5)
                    await n_t1.write_value(
                        ua.Variant(float(180.0 + 2.5 * step), ua.VariantType.Double)
                    )
                    if step == 3 and req.drop_csv_alarm:
                        await drop_sample_csv_alarm()
                await n_trig.write_value(ua.Variant(False, ua.VariantType.Boolean))

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
    reload_poller: bool = Query(default=True),
    db: AsyncSession = Depends(get_db_session),
) -> ReportSchema:
    """Update an existing ReportSchema layout configuration and reload the poller."""
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

    if reload_poller and _poller is not None:
        await restart_runtime_services(start_poller=True)

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
