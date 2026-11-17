"""
SQLAlchemy 2.0 ORM Models for the OPC-UA Automated Cycle Reporting System.

All column types, lengths, indexes, and constraints are strictly cross-compatible
between SQLite and Microsoft SQL Server.

Key Design:
- `ReportSchema`: Defines both `header_tags` (static metadata like Batch_Number read
  ONCE on cycle start) and `tags` (dynamic process values polled every 0.5s).
- `CycleRecord`: Stores cycle start/end times, status, `batch_number`, and static
  `header_data` JSON captured once at cycle start.
- `CycleTimeSeries`: Stores ONLY dynamic 0.5s process variables (no redundant batch/recipe
  strings repeated every 0.5s).
- `CycleAlarmLog`: Stores alarm events ingested from watched CSV files via `watchdog`.
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
    to S7-1500 NodeIDs.
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
    # Contains `header_tags` (read once on cycle start), `tags` (polled every 0.5s),
    # `charts`, `parameter_tables`, and `alarms`.
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

    def extract_header_nodes(self) -> dict[str, str]:
        """
        Extract static cycle metadata tags (`{tag_name: node_id}`) that do NOT
        change during a cycle (e.g., `Batch_Number`, `Recipe_Name`) and should
        only be read ONCE when `Cycle_Active` transitions to TRUE.
        """
        header_tags = self.schema_config.get("header_tags", {})
        if isinstance(header_tags, dict):
            return {
                str(k): str(v) for k, v in header_tags.items() if k and v
            }
        return {}

    def extract_polled_nodes(self) -> dict[str, str]:
        """
        Extract continuous 0.5s time-series tags (`{tag_name: node_id}`) from
        `schema_config`, excluding any static tags already listed in `header_tags`.
        """
        polled: dict[str, str] = {}
        header_keys = set(self.extract_header_nodes().keys())

        # 1. Top-level "tags" mapping
        tags = self.schema_config.get("tags", {})
        if isinstance(tags, dict):
            for tag_name, node_id in tags.items():
                if tag_name and node_id and str(tag_name) not in header_keys:
                    polled[str(tag_name)] = str(node_id)

        # 2. Inline series inside "charts"
        for chart in self.schema_config.get("charts", []):
            if isinstance(chart, dict):
                for series in chart.get("series", []):
                    if (
                        isinstance(series, dict)
                        and series.get("tag_name")
                        and series.get("node_id")
                        and str(series["tag_name"]) not in header_keys
                    ):
                        polled[str(series["tag_name"])] = str(series["node_id"])

        # 3. Inline parameters inside "parameter_tables" or "tables"
        for table_key in ("parameter_tables", "tables"):
            for table in self.schema_config.get(table_key, []):
                if isinstance(table, dict):
                    for param in table.get("parameters", []):
                        if (
                            isinstance(param, dict)
                            and param.get("tag_name")
                            and param.get("node_id")
                            and str(param["tag_name"]) not in header_keys
                        ):
                            polled[str(param["tag_name"])] = str(param["node_id"])

        # 4. Inline alarm definitions inside "alarms"
        for alarm in self.schema_config.get("alarms", []):
            if (
                isinstance(alarm, dict)
                and alarm.get("tag_name")
                and alarm.get("node_id")
                and str(alarm["tag_name"]) not in header_keys
            ):
                polled[str(alarm["tag_name"])] = str(alarm["node_id"])

        return polled


class CycleRecord(Base):
    """
    Logs every machine cycle execution, including static cycle metadata
    (`batch_number`, `header_data`) captured once on cycle start.
    """

    __tablename__ = "cycle_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    report_schema_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("report_schemas.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )

    # Non-redundant static cycle data read ONCE when Cycle_Active goes TRUE
    batch_number: Mapped[Optional[str]] = mapped_column(
        String(128), nullable=True, index=True
    )
    header_data: Mapped[Optional[dict[str, Any]]] = mapped_column(
        JSON, nullable=True
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
    csv_alarms: Mapped[list["CycleAlarmLog"]] = relationship(
        "CycleAlarmLog",
        back_populates="cycle",
        cascade="all, delete-orphan",
    )


class CycleTimeSeries(Base):
    """
    High-frequency time-series table storing continuous 0.5s numeric data points
    polled from the S7-1500 PLC during an active cycle.
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
    value: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    raw_value: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    cycle: Mapped["CycleRecord"] = relationship(
        "CycleRecord", back_populates="time_series_data"
    )


class CycleAlarmLog(Base):
    """
    Stores alarm records ingested from PLC/HMI CSV files via `watchdog`.
    Can be linked directly to an active `cycle_id` upon ingestion or matched
    to a cycle by timestamp window when generating the PDF report.
    """

    __tablename__ = "cycle_alarm_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cycle_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("cycle_records.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False, index=True
    )
    alarm_code: Mapped[str] = mapped_column(String(64), nullable=False)
    alarm_text: Mapped[str] = mapped_column(String(500), nullable=False)
    severity: Mapped[str] = mapped_column(
        String(32), default="CRITICAL", nullable=False
    )
    source_file: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    cycle: Mapped[Optional["CycleRecord"]] = relationship(
        "CycleRecord", back_populates="csv_alarms"
    )


class SystemConfig(Base):
    """
    Persists runtime PLC connection and application configuration editable from the UI.
    Single-row configuration table (id=1).
    """

    __tablename__ = "system_config"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    opcua_endpoint_url: Mapped[str] = mapped_column(
        String(255), default="opc.tcp://192.168.0.1:4840", nullable=False
    )
    opcua_username: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    opcua_password: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    poll_interval_seconds: Mapped[float] = mapped_column(
        Float, default=0.5, nullable=False
    )
    csv_watch_dir: Mapped[str] = mapped_column(
        String(500), default="./watched_alarms", nullable=False
    )
    use_mock_plc: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    active_schema_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )

