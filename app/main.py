"""
FastAPI Backend for the Automated Machine Cycle Reporting System & Web Report Designer.

Provides REST endpoints to:
- Create, read, update, and delete dynamic JSON `ReportSchema` layouts.
- Inspect completed/running `CycleRecord` entries and their time-series statistics.
- Manually trigger or re-generate PDF reports (with optional OS printing).
- Download generated cycle PDF reports.
"""

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncGenerator, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db_session, init_db
from app.models import CycleRecord, CycleStatus, CycleTimeSeries, ReportSchema
from app.report_generator import generate_cycle_pdf_report
from app.schemas import (
    CycleRecordRead,
    ReportSchemaCreate,
    ReportSchemaRead,
)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
    """Initialize database schema on startup."""
    await init_db()
    yield


app = FastAPI(
    title="Siemens S7-1500 OPC-UA Cycle Reporting API",
    description=(
        "Backend API for the Web-Based Report Designer and Automated 0.5s "
        "OPC-UA Machine Cycle PDF Reporting System."
    ),
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


@app.get("/health", tags=["System"])
async def health_check() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok", "service": "opc-ua-report-generator"}


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
    """Get a cycle record along with the count of logged 0.5s time-series rows."""
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
    return {
        "cycle": CycleRecordRead.model_validate(cycle).model_dump(),
        "timeseries_points_count": total_points,
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
