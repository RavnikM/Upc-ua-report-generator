"""
Schema-Driven PDF Report Generation Engine.

Combines:
- Static one-shot cycle header metadata (`batch_number`, `header_data`) from `CycleRecord`
- Consumed CSV alarms (`CycleAlarmLog`) from the database
- 0.5s dynamic time-series data (`CycleTimeSeries`) analyzed via Pandas & Matplotlib
- Jinja2 HTML templating + WeasyPrint PDF compilation + optional `win32print` dispatch.
"""

import asyncio
from dataclasses import dataclass
import logging
from pathlib import Path
import tempfile
from typing import Any, Optional

from jinja2 import Environment, FileSystemLoader, select_autoescape
import matplotlib

# Force headless non-GUI backend before importing pyplot
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import or_, select
from weasyprint import HTML

from app.database import get_Session_context
from app.models import (
    CycleAlarmLog,
    CycleRecord,
    CycleStatus,
    CycleTimeSeries,
    ReportSchema,
)
from app.printer import PrintJobResult, print_pdf_document
from app.schemas import (
    AlarmRuleSchema,
    ChartElementSchema,
    ParameterTableElementSchema,
    ReportLayoutConfig,
)

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
DEFAULT_REPORTS_DIR = PROJECT_ROOT / "reports"

jinja_env = Environment(
    loader=FileSystemLoader(str(TEMPLATES_DIR)),
    autoescape=select_autoescape(["html", "xml"]),
)


@dataclass
class GeneratedReportResult:
    """Summary returned after generating a cycle PDF report."""

    cycle_id: int
    pdf_path: Path
    status: CycleStatus
    triggered_alarms_count: int
    print_result: Optional[PrintJobResult] = None


def _evaluate_parameter_tables(
    df_numeric: pd.DataFrame,
    tables_cfg: list[ParameterTableElementSchema],
) -> list[dict[str, Any]]:
    """Compute Min, Max, Avg and Setpoint Tolerance checks for Parameter Tables."""
    rendered_tables: list[dict[str, Any]] = []

    for tbl in tables_cfg:
        rows_out: list[dict[str, Any]] = []
        for param in tbl.parameters:
            if not df_numeric.empty and param.tag_name in df_numeric.columns:
                series = df_numeric[param.tag_name].dropna()
            else:
                series = pd.Series(dtype=float)

            if not series.empty:
                min_v = float(series.min())
                max_v = float(series.max())
                avg_v = float(series.mean())
                min_str = f"{min_v:.2f}"
                max_str = f"{max_v:.2f}"
                avg_str = f"{avg_v:.2f}"
            else:
                min_v = max_v = avg_v = float("nan")
                min_str = max_str = avg_str = "N/A"

            in_tolerance = True
            if param.target_setpoint is not None and param.tolerance is not None:
                setpoint_display = f"{param.target_setpoint:.2f} ± {param.tolerance:.2f}"
                if not series.empty:
                    lower = param.target_setpoint - param.tolerance
                    upper = param.target_setpoint + param.tolerance
                    in_tolerance = bool((min_v >= lower) and (max_v <= upper))
            elif param.target_setpoint is not None:
                setpoint_display = f"{param.target_setpoint:.2f}"
            else:
                setpoint_display = "—"

            rows_out.append(
                {
                    "tag_name": param.tag_name,
                    "label": param.label,
                    "unit": param.unit,
                    "min_val": min_str,
                    "max_val": max_str,
                    "avg_val": avg_str,
                    "setpoint_display": setpoint_display,
                    "in_tolerance": in_tolerance,
                }
            )

        rendered_tables.append({"id": tbl.id, "title": tbl.title, "rows": rows_out})

    return rendered_tables


def _evaluate_alarms(
    df_numeric: pd.DataFrame,
    alarms_cfg: list[AlarmRuleSchema],
    poll_interval_s: float = 0.5,
) -> list[dict[str, Any]]:
    """Evaluate configured OPC-UA alarm rules against the 0.5s time-series DataFrame."""
    triggered: list[dict[str, Any]] = []
    if df_numeric.empty:
        return triggered

    for rule in alarms_cfg:
        if rule.tag_name not in df_numeric.columns:
            continue

        series = df_numeric[rule.tag_name].dropna()
        if series.empty:
            continue

        if rule.condition == "is_true":
            mask = series >= 0.5
            cond_display = "== TRUE"
        elif rule.condition == "gt" and rule.threshold is not None:
            mask = series > rule.threshold
            cond_display = f"> {rule.threshold}"
        elif rule.condition == "gte" and rule.threshold is not None:
            mask = series >= rule.threshold
            cond_display = f">= {rule.threshold}"
        elif rule.condition == "lt" and rule.threshold is not None:
            mask = series < rule.threshold
            cond_display = f"< {rule.threshold}"
        elif rule.condition == "lte" and rule.threshold is not None:
            mask = series <= rule.threshold
            cond_display = f"<= {rule.threshold}"
        else:
            continue

        violated = series[mask]
        if not violated.empty:
            first_ts = violated.index[0]
            first_ts_str = (
                first_ts.strftime("%H:%M:%S.%f")[:-3]
                if hasattr(first_ts, "strftime")
                else str(first_ts)
            )
            active_count = int(len(violated))
            peak_val = (
                "TRUE"
                if rule.condition == "is_true"
                else f"{float(violated.max()):.2f}"
            )
            triggered.append(
                {
                    "id": rule.id,
                    "tag_name": rule.tag_name,
                    "label": rule.label,
                    "severity": rule.severity,
                    "condition_display": cond_display,
                    "first_timestamp": first_ts_str,
                    "active_samples": active_count,
                    "active_duration_s": f"{active_count * poll_interval_s:.1f}",
                    "peak_value": peak_val,
                }
            )

    return triggered


def _render_charts_and_compile_pdf_sync(
    df_numeric: pd.DataFrame,
    charts_cfg: list[ChartElementSchema],
    template_context: dict[str, Any],
    output_pdf_path: Path,
) -> Path:
    """
    Synchronous worker (run inside `asyncio.to_thread`) that generates Matplotlib
    charts to temp PNGs, renders Jinja2 HTML, and compiles the PDF via WeasyPrint.
    """
    output_pdf_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="opc_report_charts_") as tmp_dir:
        tmp_dir_path = Path(tmp_dir)
        chart_images: list[dict[str, str]] = []

        if not df_numeric.empty:
            t0 = df_numeric.index[0]
            elapsed_seconds = [(ts - t0).total_seconds() for ts in df_numeric.index]
        else:
            elapsed_seconds = []

        for idx, chart_cfg in enumerate(charts_cfg):
            fig, ax = plt.subplots(figsize=(7.2, 2.55), dpi=180)
            plotted_any = False

            for s_cfg in chart_cfg.series:
                if not df_numeric.empty and s_cfg.tag_name in df_numeric.columns:
                    y_vals = df_numeric[s_cfg.tag_name]
                    legend_lbl = (
                        f"{s_cfg.label} ({s_cfg.unit})"
                        if s_cfg.unit
                        else s_cfg.label
                    )
                    plot_kwargs: dict[str, Any] = {
                        "label": legend_lbl,
                        "linewidth": 1.8,
                        "marker": "o" if len(y_vals) <= 25 else None,
                        "markersize": 3.5,
                    }
                    if s_cfg.color:
                        plot_kwargs["color"] = s_cfg.color

                    ax.plot(elapsed_seconds, y_vals, **plot_kwargs)
                    plotted_any = True

            ax.set_title(chart_cfg.title, fontsize=10, fontweight="bold", pad=8)
            ax.set_xlabel("Cycle Elapsed Time (seconds)", fontsize=8.5)
            ax.set_ylabel(chart_cfg.y_axis_label, fontsize=8.5)
            ax.tick_params(axis="both", labelsize=8)
            ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.6)

            if plotted_any:
                ax.legend(loc="best", fontsize=8, frameon=True)
            else:
                ax.text(
                    0.5,
                    0.5,
                    "No time-series data recorded for configured tags",
                    ha="center",
                    va="center",
                    transform=ax.transAxes,
                    fontsize=9,
                    color="#64748b",
                )

            fig.tight_layout()
            chart_file = tmp_dir_path / f"chart_{idx}_{chart_cfg.id}.png"
            fig.savefig(chart_file, format="png", dpi=180)
            plt.close(fig)

            chart_images.append(
                {
                    "id": chart_cfg.id,
                    "title": chart_cfg.title,
                    "file_uri": chart_file.resolve().as_uri(),
                }
            )

        full_context = {**template_context, "chart_images": chart_images}
        template = jinja_env.get_template("cycle_report.html")
        rendered_html = template.render(**full_context)

        HTML(string=rendered_html, base_url=str(tmp_dir_path)).write_pdf(
            str(output_pdf_path)
        )

    return output_pdf_path


async def generate_cycle_pdf_report(
    cycle_id: int,
    reports_dir: Path = DEFAULT_REPORTS_DIR,
    force_print: Optional[bool] = None,
) -> GeneratedReportResult:
    """
    Generate (and optionally print) a PDF report for `cycle_id`, combining:
    - One-shot `batch_number` and `header_data` from `CycleRecord`
    - CSV-ingested alarms from `CycleAlarmLog`
    - 0.5s time-series data from `CycleTimeSeries`
    """
    async with get_Session_context() as session:
        cycle = await session.get(CycleRecord, cycle_id)
        if cycle is None:
            raise ValueError(f"CycleRecord id={cycle_id} not found.")

        schema_row = await session.get(ReportSchema, cycle.report_schema_id)
        if schema_row is None:
            raise ValueError(
                f"ReportSchema id={cycle.report_schema_id} not found for cycle_id={cycle_id}."
            )

        ts_result = await session.execute(
            select(CycleTimeSeries)
            .where(CycleTimeSeries.cycle_id == cycle_id)
            .order_by(CycleTimeSeries.timestamp, CycleTimeSeries.id)
        )
        ts_rows = list(ts_result.scalars().all())

        start_time = cycle.start_time
        end_time = cycle.end_time or cycle.start_time

        # Query CSV alarms attached to this cycle_id OR falling within its [start_time, end_time] window
        csv_alarms_result = await session.execute(
            select(CycleAlarmLog)
            .where(
                or_(
                    CycleAlarmLog.cycle_id == cycle_id,
                    (
                        CycleAlarmLog.cycle_id.is_(None)
                        & (CycleAlarmLog.timestamp >= start_time)
                        & (CycleAlarmLog.timestamp <= end_time)
                    ),
                )
            )
            .order_by(CycleAlarmLog.timestamp, CycleAlarmLog.id)
        )
        csv_alarm_rows = list(csv_alarms_result.scalars().all())

        # Ensure any matched unattached CSV alarms get linked to this cycle_id
        for alm_row in csv_alarm_rows:
            if alm_row.cycle_id is None:
                alm_row.cycle_id = cycle_id

        report_name = schema_row.report_name
        trigger_node = schema_row.trigger_node
        layout_cfg = ReportLayoutConfig.model_validate(schema_row.schema_config)
        batch_number = cycle.batch_number or "N/A"
        header_data = cycle.header_data or {}

    try:
        if ts_rows:
            raw_df = pd.DataFrame(
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
            df_numeric = raw_df.pivot_table(
                index="timestamp",
                columns="tag_name",
                values="value",
                aggfunc="first",
            ).sort_index()
            total_snapshots = int(len(df_numeric))
        else:
            df_numeric = pd.DataFrame()
            total_snapshots = 0

        # Format static one-shot header metadata (excluding Batch_Number which has its own field)
        header_parts = [
            f"{k}: {v}"
            for k, v in header_data.items()
            if k not in ("Batch_Number", "batch_number") and v is not None
        ]
        header_metadata_summary = (
            " | ".join(header_parts) if header_parts else "Standard"
        )

        duration_seconds = round(
            max(0.0, (end_time - start_time).total_seconds()), 2
        )

        parameter_tables = _evaluate_parameter_tables(
            df_numeric, layout_cfg.parameter_tables
        )
        triggered_alarms = _evaluate_alarms(df_numeric, layout_cfg.alarms)

        formatted_csv_alarms = [
            {
                "alarm_code": r.alarm_code,
                "alarm_text": r.alarm_text,
                "severity": r.severity,
                "source_file": r.source_file,
                "timestamp_str": r.timestamp.strftime("%H:%M:%S.%f")[:-3],
            }
            for r in csv_alarm_rows
        ]
        total_alarms_count = len(formatted_csv_alarms) + len(triggered_alarms)

        safe_report_name = "".join(
            c if c.isalnum() or c in ("-", "_") else "_" for c in report_name
        )
        ts_slug = start_time.strftime("%Y%m%d_%H%M%S")
        output_pdf_path = (
            reports_dir / f"cycle_{cycle_id}_{safe_report_name}_{ts_slug}.pdf"
        )

        template_context: dict[str, Any] = {
            "header_title": layout_cfg.header_title,
            "machine_name": layout_cfg.machine_name,
            "report_name": report_name,
            "trigger_node": trigger_node,
            "cycle": {"id": cycle_id},
            "batch_number": batch_number,
            "header_metadata_summary": header_metadata_summary,
            "start_time_str": start_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "end_time_str": end_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "duration_seconds": f"{duration_seconds:.2f}",
            "total_snapshots": total_snapshots,
            "parameter_tables": parameter_tables,
            "triggered_alarms": triggered_alarms,
            "csv_alarms": formatted_csv_alarms,
            "total_alarms_count": total_alarms_count,
        }

        await asyncio.to_thread(
            _render_charts_and_compile_pdf_sync,
            df_numeric,
            layout_cfg.charts,
            template_context,
            output_pdf_path,
        )

        should_print = (
            force_print if force_print is not None else layout_cfg.auto_print
        )
        print_res: Optional[PrintJobResult] = None
        final_status = CycleStatus.REPORT_GENERATED

        if should_print:
            print_res = await asyncio.to_thread(
                print_pdf_document,
                output_pdf_path,
                layout_cfg.printer_name,
            )
            if print_res.success:
                final_status = CycleStatus.PRINTED

        async with get_Session_context() as session:
            cycle_to_update = await session.get(CycleRecord, cycle_id)
            if cycle_to_update is not None:
                cycle_to_update.pdf_file_path = str(output_pdf_path.resolve())
                cycle_to_update.status = final_status
                cycle_to_update.error_message = None

        logger.info(
            "Generated PDF report for cycle_id=%d (batch=%s) -> %s (status=%s, csv_alarms=%d, opc_alarms=%d)",
            cycle_id,
            batch_number,
            output_pdf_path.name,
            final_status.value,
            len(formatted_csv_alarms),
            len(triggered_alarms),
        )
        return GeneratedReportResult(
            cycle_id=cycle_id,
            pdf_path=output_pdf_path,
            status=final_status,
            triggered_alarms_count=total_alarms_count,
            print_result=print_res,
        )

    except Exception as exc:
        logger.exception(
            "PDF report generation failed for cycle_id=%d: %s", cycle_id, exc
        )
        async with get_Session_context() as session:
            cycle_to_update = await session.get(CycleRecord, cycle_id)
            if cycle_to_update is not None:
                cycle_to_update.status = CycleStatus.FAILED
                cycle_to_update.error_message = str(exc)
        raise
