"""
SQLAlchemy 2.0 ORM Models for the OPC-UA Automated Cycle Reporting System.

All column types, lengths, indexes, and constraints are strictly cross-compatible
between SQLite and Microsoft SQL Server (avoiding dialect-specific types like
PostgreSQL JSONB or unindexed NVARCHAR(MAX) keys).
"""

import enum
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


def utc_now() -> datetime:
    """Return current UTC timestamp."""
    return datetime.now(timezone.utc)


class CycleStatus(str, enum.Enum):
    """Lifecycle states of a machine production cycle."""

    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    REPORT_GENERATED = "REPORT_GENERATED"
    PRINTED = "PRINTED"
    FAILED = "FAILED"


class ReportSchema(Base):
    """
    Stores the JSON configuration of a dynamic report layout, the associated
    OPC-UA trigger node, and metadata for mapping visual report elements
    (Charts, Parameter Tables, Alarms) to S7-1500 NodeIDs.
    """

    __tablename__ = "report_schemas"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    report_name: Mapped[str] = mapped_column(
        String(150), nullable=False, unique=True, index=True
    )
    description: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    # Full OPC-UA NodeId string for the boolean cycle trigger
    # Example: ns=3;s="Cycle_Control"."Cycle_Active"
    trigger_node: Mapped[str] = mapped_column(String(255), nullable=False, index=True)

    # Dynamic JSON layout configuration:
    # Maps charts, parameter tables, and alarms to OPC-UA NodeID strings.
    # SQLAlchemy's generic JSON type maps to JSON on SQLite and NVARCHAR(MAX)
    # with JSON serialization on MS SQL Server.
    schema_config: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )

    # Relationships
    cycles: Mapped[list["CycleRecord"]] = relationship(
        "CycleRecord",
        back_populates="report_schema",
        cascade="all, delete-orphan",
    )

    def extract_polled_nodes(self) -> dict[str, str]:
        """
        Helper method to extract a deduplicated mapping of {tag_name: node_id}
        from `schema_config` for bulk OPC-UA reads during an active cycle.

        Expected `schema_config` structure example:
        {
            "tags": {
                "Temp_Zone1": "ns=3;s=\"Cycle_Data\".\"Temp_Zone1\"",
                "Pressure_Main": "ns=3;s=\"Cycle_Data\".\"Pressure_Main\""
            },
            "charts": [...],
            "tables": [...],
            "alarms": [...]
        }
        """
        tags = self.schema_config.get("tags", {})
        if isinstance(tags, dict):
            return {str(k): str(v) for k, v in tags.items()}
        return {}


class CycleRecord(Base):
    """
    Logs every machine cycle execution (start time, end time, associated
    report schema ID, status, and generated PDF path).
    """

    __tablename__ = "cycle_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    report_schema_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("report_schemas.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )

    start_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False, index=True
    )
    end_time: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    status: Mapped[CycleStatus] = mapped_column(
        Enum(CycleStatus, native_enum=False, length=32),
        default=CycleStatus.RUNNING,
        nullable=False,
        index=True,
    )

    pdf_file_path: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Relationships
    report_schema: Mapped["ReportSchema"] = relationship(
        "ReportSchema", back_populates="cycles"
    )
    time_series_data: Mapped[list["CycleTimeSeries"]] = relationship(
        "CycleTimeSeries",
        back_populates="cycle",
        cascade="all, delete-orphan",
    )


class CycleTimeSeries(Base):
    """
    High-frequency time-series table storing continuous 0.5s data points
    polled from the S7-1500 PLC during an active cycle.

    Includes a composite index on (cycle_id, tag_name, timestamp) for fast
    slice queries when constructing Pandas DataFrames for report generation.
    """

    __tablename__ = "cycle_time_series"
    __table_args__ = (
        Index(
            "ix_cycle_timeseries_cycle_tag_ts",
            "cycle_id",
            "tag_name",
            "timestamp",
        ),
    )

    # Note: Integer primary key with autoincrement is used for 100% compatibility
    # with both SQLite's ROWID autoincrement and MS SQL Server IDENTITY.
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    cycle_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("cycle_records.id", ondelete="CASCADE"),
        nullable=False,
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    tag_name: Mapped[str] = mapped_column(String(128), nullable=False)

    # Numeric value for charting/aggregation, plus optional raw string fallback
    # for boolean/string/status tags.
    value: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    raw_value: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Relationships
    cycle: Mapped["CycleRecord"] = relationship(
        "CycleRecord", back_populates="time_series_data"
    )
