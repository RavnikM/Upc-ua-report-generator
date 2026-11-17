"""
Pydantic v2 Schemas for Schema-Driven Report Mapping & API Serialization.

Validates JSON report definitions that map visual report elements
(Charts, Parameter Tables, Alarms) to Siemens S7-1500 OPC-UA NodeID strings.
"""

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from app.models import CycleStatus


class ChartSeriesSchema(BaseModel):
    """Maps a single plotted line on a time-series chart to an OPC-UA tag."""

    tag_name: str = Field(..., description="Logical tag key, e.g. 'Temp_Zone1'")
    label: str = Field(..., description="Legend label, e.g. 'Zone 1 Temperature (°C)'")
    node_id: Optional[str] = Field(
        default=None,
        description='Optional inline OPC-UA NodeID, e.g. ns=3;s="Cycle_Data"."Temp_Zone1"',
    )
    unit: str = Field(default="", description="Engineering unit, e.g. '°C', 'bar', 'RPM'")
    color: Optional[str] = Field(default=None, description="Hex or Matplotlib color string")


class ChartElementSchema(BaseModel):
    """Defines a time-series chart block in the PDF report."""

    id: str = Field(..., description="Unique chart identifier within the report")
    title: str = Field(..., description="Chart title rendered on the report")
    y_axis_label: str = Field(default="Value", description="Y-axis label")
    series: list[ChartSeriesSchema] = Field(..., min_length=1)


class ParameterRowSchema(BaseModel):
    """Defines a statistical summary row in a Parameter Table."""

    tag_name: str = Field(..., description="Logical tag key")
    label: str = Field(..., description="Human-readable parameter name")
    node_id: Optional[str] = Field(default=None)
    unit: str = Field(default="")
    aggregations: list[Literal["min", "max", "avg", "first", "last", "std"]] = Field(
        default_factory=lambda: ["min", "max", "avg"]
    )
    target_setpoint: Optional[float] = Field(default=None)
    tolerance: Optional[float] = Field(default=None)


class ParameterTableElementSchema(BaseModel):
    """Defines a parameter summary table block in the PDF report."""

    id: str = Field(..., description="Unique table identifier")
    title: str = Field(..., description="Table section header")
    parameters: list[ParameterRowSchema] = Field(..., min_length=1)


class AlarmRuleSchema(BaseModel):
    """
    Defines an alarm condition evaluated over the 0.5s cycle time-series data
    (either a boolean PLC alarm tag or a numeric threshold rule).
    """

    id: str = Field(..., description="Unique alarm rule identifier")
    tag_name: str = Field(..., description="Monitored tag name")
    label: str = Field(..., description="Alarm description shown when triggered")
    node_id: Optional[str] = Field(default=None)
    condition: Literal["is_true", "gt", "lt", "gte", "lte"] = Field(default="is_true")
    threshold: Optional[float] = Field(
        default=None,
        description="Numeric threshold for gt/lt/gte/lte conditions",
    )
    severity: Literal["INFO", "WARNING", "CRITICAL"] = Field(default="CRITICAL")


class ReportLayoutConfig(BaseModel):
    """
    Root JSON configuration stored in `ReportSchema.schema_config`.
    """

    header_title: str = Field(default="Automated Machine Cycle Report")
    machine_name: str = Field(default="Siemens S7-1500 Line 1")
    auto_print: bool = Field(default=False)
    printer_name: Optional[str] = Field(default=None)

    # Central lookup of {tag_name: opc_ua_node_id}
    tags: dict[str, str] = Field(
        default_factory=dict,
        description='Mapping of logical tag names to OPC-UA NodeId strings',
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


class CycleRecordRead(BaseModel):
    """Serialized CycleRecord database entity."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    report_schema_id: int
    start_time: datetime
    end_time: Optional[datetime]
    status: CycleStatus
    pdf_file_path: Optional[str]
    error_message: Optional[str]
