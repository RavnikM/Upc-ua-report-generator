"""
Pydantic v2 Schemas for Schema-Driven Report Mapping & API Serialization.
"""

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from app.models import CycleStatus


class ChartSeriesSchema(BaseModel):
    """Maps a single plotted line on a time-series chart to an OPC-UA tag."""

    tag_name: str = Field(..., description="Logical tag key, e.g. 'Temp_Zone1'")
    label: str = Field(..., description="Legend label, e.g. 'Zone 1 Temperature (°C)'")
    node_id: Optional[str] = Field(default=None)
    unit: str = Field(default="", description="Engineering unit, e.g. '°C', 'bar', 'RPM'")
    color: Optional[str] = Field(default=None)


class ChartElementSchema(BaseModel):
    """Defines a time-series chart block in the PDF report."""

    id: str = Field(...)
    title: str = Field(...)
    y_axis_label: str = Field(default="Value")
    series: list[ChartSeriesSchema] = Field(..., min_length=1)


class ParameterRowSchema(BaseModel):
    """Defines a statistical summary row in a Parameter Table."""

    tag_name: str = Field(...)
    label: str = Field(...)
    node_id: Optional[str] = Field(default=None)
    unit: str = Field(default="")
    aggregations: list[Literal["min", "max", "avg", "first", "last", "std"]] = Field(
        default_factory=lambda: ["min", "max", "avg"]
    )
    target_setpoint: Optional[float] = Field(default=None)
    tolerance: Optional[float] = Field(default=None)


class ParameterTableElementSchema(BaseModel):
    """Defines a parameter summary table block in the PDF report."""

    id: str = Field(...)
    title: str = Field(...)
    parameters: list[ParameterRowSchema] = Field(..., min_length=1)


class AlarmRuleSchema(BaseModel):
    """Defines an optional threshold/tag alarm evaluated over time-series data."""

    id: str = Field(...)
    tag_name: str = Field(...)
    label: str = Field(...)
    node_id: Optional[str] = Field(default=None)
    condition: Literal["is_true", "gt", "lt", "gte", "lte"] = Field(default="is_true")
    threshold: Optional[float] = Field(default=None)
    severity: Literal["INFO", "WARNING", "CRITICAL"] = Field(default="CRITICAL")


class ReportLayoutConfig(BaseModel):
    """
    Root JSON configuration stored in `ReportSchema.schema_config`.
    Separates `header_tags` (read ONCE on cycle start, such as Batch_Number)
    from `tags` (polled every 0.5s during an active cycle).
    """

    header_title: str = Field(default="Automated Machine Cycle Report")
    machine_name: str = Field(default="Siemens S7-1500 Line 1")
    auto_print: bool = Field(default=False)
    printer_name: Optional[str] = Field(default=None)

    # Static cycle tags read ONCE at cycle start (e.g. Batch_Number, Recipe_Name)
    header_tags: dict[str, str] = Field(
        default_factory=dict,
        description="Static tags read once when Cycle_Active becomes True",
    )

    # Dynamic process tags polled every 0.5s during an active cycle
    tags: dict[str, str] = Field(
        default_factory=dict,
        description="Dynamic process tags polled every 0.5s while Cycle_Active is True",
    )
    charts: list[ChartElementSchema] = Field(default_factory=list)
    parameter_tables: list[ParameterTableElementSchema] = Field(default_factory=list)
    alarms: list[AlarmRuleSchema] = Field(default_factory=list)


class ReportSchemaCreate(BaseModel):
    """Payload to create or update a ReportSchema."""

    report_name: str = Field(..., max_length=150)
    description: Optional[str] = Field(default=None, max_length=500)
    trigger_node: str = Field(..., max_length=255)
    schema_config: ReportLayoutConfig
    is_active: bool = True


class ReportSchemaRead(BaseModel):
    """Serialized ReportSchema database entity."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    report_name: str
    description: Optional[str]
    trigger_node: str
    schema_config: dict[str, Any]
    is_active: bool
    created_at: datetime
    updated_at: datetime


class CycleAlarmLogRead(BaseModel):
    """Serialized CycleAlarmLog database entity."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    cycle_id: Optional[int]
    timestamp: datetime
    alarm_code: str
    alarm_text: str
    severity: str
    source_file: Optional[str]


class CycleRecordRead(BaseModel):
    """Serialized CycleRecord database entity."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    report_schema_id: int
    batch_number: Optional[str]
    header_data: Optional[dict[str, Any]]
    start_time: datetime
    end_time: Optional[datetime]
    status: CycleStatus
    pdf_file_path: Optional[str]
    error_message: Optional[str]


class SystemConfigUpdate(BaseModel):
    """Payload to update PLC connection & runtime settings from the UI."""

    opcua_endpoint_url: str = Field(..., max_length=255)
    opcua_username: Optional[str] = Field(default=None, max_length=128)
    opcua_password: Optional[str] = Field(default=None, max_length=255)
    poll_interval_seconds: float = Field(default=0.5, ge=0.1, le=60.0)
    csv_watch_dir: str = Field(default="./watched_alarms", max_length=500)
    use_mock_plc: bool = Field(default=False)
    active_schema_id: Optional[int] = Field(default=None)


class SystemConfigRead(BaseModel):
    """Serialized SystemConfig entity."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    opcua_endpoint_url: str
    opcua_username: Optional[str]
    opcua_password: Optional[str]
    poll_interval_seconds: float
    csv_watch_dir: str
    use_mock_plc: bool
    active_schema_id: Optional[int]
    updated_at: datetime


class PLCConnectionTestRequest(BaseModel):
    """Optional override parameters when clicking 'Test PLC Connection' in the UI."""

    opcua_endpoint_url: Optional[str] = None
    opcua_username: Optional[str] = None
    opcua_password: Optional[str] = None
    use_mock_plc: Optional[bool] = None


class PLCNodeTestRequest(BaseModel):
    """Payload to bulk test-read one or more OPC-UA NodeIds on the PLC."""

    node_ids: list[str]
    opcua_endpoint_url: Optional[str] = None
    opcua_username: Optional[str] = None
    opcua_password: Optional[str] = None
    use_mock_plc: Optional[bool] = None

