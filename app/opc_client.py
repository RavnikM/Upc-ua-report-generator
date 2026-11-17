"""
Asynchronous OPC-UA Client & 0.5s State-Machine Polling Engine for Siemens S7-1500.

Implements a deterministic 3-state machine with non-redundant static tag reading:
- State 1 (IDLE): Polls the boolean `Cycle_Active` trigger node every 0.5s.
- Transition IDLE -> ACTIVE (Rising Edge): Executes a ONE-SHOT bulk read of static
  `header_nodes_map` (e.g., `Batch_Number`, `Recipe_Name`) and saves them directly on
  `CycleRecord` so static cycle metadata is never redundantly polled every 0.5s.
- State 2 (ACTIVE): Executes single-request bulk reads (`client.read_nodes()`)
  for dynamic process `data_nodes_map` every 0.5s and logs them to `CycleTimeSeries`.
- State 3 (COMPLETION): Triggered on the falling edge (TRUE -> FALSE) of
  `Cycle_Active`, finalizing the cycle record and dispatching PDF report generation.
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import enum
import logging
import time
from typing import Any, Optional

from asyncua import Client, Node, ua
from asyncua.ua.uaerrors import UaError
from sqlalchemy import insert

from app.database import get_Session_context
from app.models import CycleRecord, CycleStatus, CycleTimeSeries

logger = logging.getLogger(__name__)


class PollerState(str, enum.Enum):
    """Finite state machine states for the OPC-UA cycle monitor."""

    IDLE = "IDLE"
    ACTIVE = "ACTIVE"
    COMPLETION = "COMPLETION"


@dataclass(frozen=True)
class CyclePollerConfig:
    """Configuration for a monitored machine cycle schema."""

    report_schema_id: int
    trigger_node_id: str
    # Dynamic process tags polled every 0.5s while Cycle_Active == True
    data_nodes_map: dict[str, str]
    # Static cycle metadata tags (e.g. Batch_Number, Recipe_Name) read ONCE on cycle start
    header_nodes_map: dict[str, str] = field(default_factory=dict)
    poll_interval: float = 0.5
    reconnect_delay: float = 5.0
    persist_to_db: bool = True
    auto_generate_pdf: bool = True
    pdf_simulation_delay: float = 0.05


@dataclass
class PolledSampleBatch:
    """In-memory representation of a single 0.5s bulk-read snapshot."""

    cycle_id: int
    timestamp: datetime
    readings: dict[str, Any] = field(default_factory=dict)


class S7CyclePoller:
    """
    High-efficiency OPC-UA polling client for a Siemens S7-1500 PLC.

    Separates one-shot cycle header reads (`header_nodes_map`, read once at cycle start)
    from continuous 0.5s time-series bulk reads (`data_nodes_map`).
    """

    def __init__(
        self,
        endpoint_url: str,
        config: CyclePollerConfig,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        self.endpoint_url = endpoint_url
        self.config = config
        self.username = username
        self.password = password

        self.state: PollerState = PollerState.IDLE
        self._stop_event = asyncio.Event()
        self._background_tasks: set[asyncio.Task[Any]] = set()

        # Cached asyncua Node references initialized upon connection
        self._trigger_node: Optional[Node] = None
        self._header_tag_names: list[str] = []
        self._header_nodes: list[Node] = []
        self._tag_names: list[str] = []
        self._data_nodes: list[Node] = []

        # Diagnostics & telemetry counters
        self.invalid_data_nodes: dict[str, str] = {}
        self.reconnect_count: int = 0
        self._has_connected_once: bool = False

        # Active cycle tracking
        self._active_cycle_id: Optional[int] = None
        self.active_header_data: dict[str, Any] = {}
        self._in_memory_cycle_counter: int = 0
        self.recorded_batches: list[PolledSampleBatch] = []
        self.completed_cycle_ids: list[int] = []

    @property
    def active_cycle_id(self) -> Optional[int]:
        """Return currently running cycle ID if ACTIVE, else None."""
        return self._active_cycle_id

    def _prepare_node_references(self, client: Client) -> None:
        """
        Pre-resolve `asyncua.Node` objects for the trigger tag, one-shot header tags,
        and 0.5s process data tags.
        """
        if not hasattr(client, "read_nodes"):
            setattr(client, "read_nodes", client.read_values)

        self._trigger_node = client.get_node(self.config.trigger_node_id)

        self._header_tag_names = list(self.config.header_nodes_map.keys())
        self._header_nodes = [
            client.get_node(node_id_str)
            for node_id_str in self.config.header_nodes_map.values()
        ]

        self._tag_names = list(self.config.data_nodes_map.keys())
        self._data_nodes = [
            client.get_node(node_id_str)
            for node_id_str in self.config.data_nodes_map.values()
        ]
        logger.info(
            "Prepared OPC-UA handles: trigger='%s', one-shot header tags=%d (%s), 0.5s bulk tags=%d (%s)",
            self.config.trigger_node_id,
            len(self._header_nodes),
            ", ".join(self._header_tag_names) or "none",
            len(self._data_nodes),
            ", ".join(self._tag_names),
        )

    async def _validate_nodes(self, client: Client) -> None:
        """
        Perform a single bulk attribute read upon connection to verify that the
        trigger NodeId, header NodeIds, and data NodeIds exist in the S7-1500 address space.
        """
        assert self._trigger_node is not None
        all_nodes: list[Node] = [
            self._trigger_node,
            *self._header_nodes,
            *self._data_nodes,
        ]
        data_values = await client.read_attributes(all_nodes, ua.AttributeIds.Value)

        trigger_dv = data_values[0]
        if not trigger_dv.StatusCode.is_good():
            msg = (
                f"Trigger node '{self.config.trigger_node_id}' is invalid on PLC: "
                f"{trigger_dv.StatusCode.name} ({trigger_dv.StatusCode.doc})"
            )
            logger.error(msg)
            raise UaError(msg)

        self.invalid_data_nodes.clear()
        combined_names = [*self._header_tag_names, *self._tag_names]
        combined_map = {**self.config.header_nodes_map, **self.config.data_nodes_map}
        for tag_name, dv in zip(combined_names, data_values[1:]):
            if not dv.StatusCode.is_good():
                node_id_str = combined_map[tag_name]
                self.invalid_data_nodes[tag_name] = dv.StatusCode.name
                logger.warning(
                    "Configured tag '%s' (%s) returned non-good StatusCode: %s (%s)",
                    tag_name,
                    node_id_str,
                    dv.StatusCode.name,
                    dv.StatusCode.doc,
                )

    async def _read_header_nodes_once(self, client: Client) -> dict[str, Any]:
        """
        Perform a single bulk read of static cycle metadata tags (`header_nodes_map`)
        when a cycle starts (e.g., `Batch_Number`, `Recipe_Name`).
        """
        if not self._header_nodes:
            return {}
        bulk_read_fn = getattr(client, "read_nodes", client.read_values)
        values: list[Any] = await bulk_read_fn(self._header_nodes)
        header_dict = dict(zip(self._header_tag_names, values))
        logger.info("[ONE-SHOT HEADER READ] %s", header_dict)
        return header_dict

    async def start(self) -> None:
        """Main supervision loop with automatic reconnection to the S7-1500 PLC."""
        self._stop_event.clear()
        logger.info("Starting S7CyclePoller targeting %s", self.endpoint_url)

        while not self._stop_event.is_set():
            client = Client(url=self.endpoint_url)
            if self.username and self.password:
                client.set_user(self.username)
                client.set_password(self.password)

            try:
                async with client:
                    if self._has_connected_once:
                        self.reconnect_count += 1
                        logger.info(
                            "Reconnected (#%d) to S7-1500 OPC-UA server at %s (resuming in state=%s)",
                            self.reconnect_count,
                            self.endpoint_url,
                            self.state.value,
                        )
                    else:
                        self._has_connected_once = True
                        logger.info(
                            "Connected to S7-1500 OPC-UA server at %s",
                            self.endpoint_url,
                        )

                    self._prepare_node_references(client)
                    await self._validate_nodes(client)
                    await self._run_state_machine_loop(client)

            except (UaError, OSError, asyncio.TimeoutError) as exc:
                if self._stop_event.is_set():
                    break
                logger.error(
                    "OPC-UA communication error (%s). Reconnecting in %.1fs...",
                    exc,
                    self.config.reconnect_delay,
                )
                await asyncio.sleep(self.config.reconnect_delay)
            except asyncio.CancelledError:
                logger.info("OPC-UA polling loop cancelled; shutting down cleanly.")
                break
            except Exception as exc:
                if self._stop_event.is_set():
                    break
                logger.exception("Unexpected error in OPC-UA poller: %s", exc)
                await asyncio.sleep(self.config.reconnect_delay)

    async def stop(self) -> None:
        """Signal the polling loop to terminate and wait for background tasks."""
        logger.info("Stopping S7CyclePoller...")
        self._stop_event.set()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)

    async def _run_state_machine_loop(self, client: Client) -> None:
        """Execute the 0.5-second drift-compensated state machine loop."""
        assert self._trigger_node is not None, "Trigger node must be initialized."

        while not self._stop_event.is_set():
            loop_start_monotonic = time.monotonic()

            if self.state == PollerState.IDLE:
                # STATE 1 (IDLE): Poll boolean `Cycle_Active` tag
                cycle_active = bool(await self._trigger_node.read_value())

                if cycle_active:
                    # Read static header tags (Batch_Number, Recipe_Name, etc.) ONCE!
                    header_data = await self._read_header_nodes_once(client)
                    self.active_header_data = header_data
                    self._active_cycle_id = await self._on_cycle_start(header_data)
                    self.state = PollerState.ACTIVE
                    logger.info(
                        "[STATE: IDLE -> ACTIVE] Rising edge on Cycle_Active! "
                        "Started cycle_id=%d (header=%s). Executing t=0 bulk read.",
                        self._active_cycle_id,
                        header_data,
                    )
                    await self._poll_active_cycle_step(client)

                    if self.state == PollerState.COMPLETION:
                        await self._handle_completion_transition()

            elif self.state == PollerState.ACTIVE:
                # STATE 2 (ACTIVE): Bulk-read trigger + dynamic data tags
                await self._poll_active_cycle_step(client)

                if self.state == PollerState.COMPLETION:
                    await self._handle_completion_transition()

            elif self.state == PollerState.COMPLETION:
                # STATE 3 (COMPLETION): Finalize cycle & dispatch PDF job
                await self._handle_completion_transition()

            elapsed = time.monotonic() - loop_start_monotonic
            sleep_duration = max(0.0, self.config.poll_interval - elapsed)
            if elapsed > self.config.poll_interval:
                logger.warning(
                    "Poll iteration took %.3fs, exceeding %.3fs target interval!",
                    elapsed,
                    self.config.poll_interval,
                )
            await asyncio.sleep(sleep_duration)

    async def _handle_completion_transition(self) -> None:
        """Process State 3 (COMPLETION) and return immediately to State 1 (IDLE)."""
        completed_cycle_id = self._active_cycle_id
        self._active_cycle_id = None
        self.active_header_data = {}
        self.state = PollerState.IDLE

        if completed_cycle_id is not None:
            await self._on_cycle_complete(completed_cycle_id)

        logger.info("[STATE: COMPLETION -> IDLE] Monitoring for next cycle.")

    async def _poll_active_cycle_step(self, client: Client) -> None:
        """
        Perform a single-request bulk read of `[trigger_node, *data_nodes]`.
        Static header tags (like `Batch_Number`) are excluded from this 0.5s loop.
        """
        assert self._trigger_node is not None
        assert self._active_cycle_id is not None

        all_nodes: list[Node] = [self._trigger_node, *self._data_nodes]
        sample_timestamp = datetime.now(timezone.utc)

        bulk_read_fn = getattr(client, "read_nodes", client.read_values)
        values: list[Any] = await bulk_read_fn(all_nodes)

        cycle_active = bool(values[0])
        tag_values = values[1:]

        if not cycle_active:
            logger.info(
                "[STATE: ACTIVE -> COMPLETION] Falling edge detected on Cycle_Active "
                "for cycle_id=%d.",
                self._active_cycle_id,
            )
            self.state = PollerState.COMPLETION
            return

        readings: dict[str, Any] = dict(zip(self._tag_names, tag_values))
        await self.log_timeseries_batch(
            cycle_id=self._active_cycle_id,
            timestamp=sample_timestamp,
            readings=readings,
        )

    # =====================================================================
    # Database & PDF Trigger Functions
    # =====================================================================

    async def _on_cycle_start(
        self, header_data: Optional[dict[str, Any]] = None
    ) -> int:
        """
        Create a new `CycleRecord` row in the database when `Cycle_Active` goes TRUE,
        persisting the one-shot `batch_number` and `header_data` dictionary.
        """
        if not self.config.persist_to_db:
            self._in_memory_cycle_counter += 1
            return self._in_memory_cycle_counter

        clean_header = header_data or {}
        # Extract batch_number if present under common key names
        batch_val: Optional[str] = None
        for key in ("Batch_Number", "batch_number", "BatchNumber", "Batch"):
            if key in clean_header and clean_header[key] is not None:
                batch_val = str(clean_header[key])
                break

        async with get_Session_context() as session:
            new_cycle = CycleRecord(
                report_schema_id=self.config.report_schema_id,
                batch_number=batch_val,
                header_data=clean_header if clean_header else None,
                start_time=datetime.now(timezone.utc),
                status=CycleStatus.RUNNING,
            )
            session.add(new_cycle)
            await session.flush()
            return new_cycle.id

    async def log_timeseries_batch(
        self,
        cycle_id: int,
        timestamp: datetime,
        readings: dict[str, Any],
    ) -> None:
        """Bulk-insert a 0.5-second snapshot of dynamic tags into `CycleTimeSeries`."""
        if not readings:
            return

        self.recorded_batches.append(
            PolledSampleBatch(
                cycle_id=cycle_id,
                timestamp=timestamp,
                readings=dict(readings),
            )
        )

        formatted_vals = ", ".join(
            f"{k}={v:.2f}" if isinstance(v, float) else f"{k}={v}"
            for k, v in readings.items()
        )
        logger.info(
            "[BULK READ | cycle_id=%d | %s] %s",
            cycle_id,
            timestamp.strftime("%H:%M:%S.%f")[:-3],
            formatted_vals,
        )

        if not self.config.persist_to_db:
            return

        rows_to_insert: list[dict[str, Any]] = []
        for tag_name, raw_val in readings.items():
            numeric_val: Optional[float] = None
            if isinstance(raw_val, (int, float)) and not isinstance(raw_val, bool):
                numeric_val = float(raw_val)
            elif isinstance(raw_val, bool):
                numeric_val = 1.0 if raw_val else 0.0

            rows_to_insert.append(
                {
                    "cycle_id": cycle_id,
                    "timestamp": timestamp,
                    "tag_name": tag_name,
                    "value": numeric_val,
                    "raw_value": str(raw_val) if raw_val is not None else None,
                }
            )

        async with get_Session_context() as session:
            await session.execute(insert(CycleTimeSeries), rows_to_insert)

    async def _on_cycle_complete(self, cycle_id: int) -> None:
        """
        Mark the `CycleRecord` as COMPLETED, link any unattached CSV alarms that
        arrived during this cycle, and spawn the async PDF report task.
        """
        end_timestamp = datetime.now(timezone.utc)
        if self.config.persist_to_db:
            from sqlalchemy import update
            from app.models import CycleAlarmLog

            async with get_Session_context() as session:
                cycle = await session.get(CycleRecord, cycle_id)
                if cycle is not None:
                    cycle.end_time = end_timestamp
                    cycle.status = CycleStatus.COMPLETED
                    # Link any CSV alarms that arrived during this cycle's window
                    await session.execute(
                        update(CycleAlarmLog)
                        .where(
                            CycleAlarmLog.cycle_id.is_(None),
                            CycleAlarmLog.timestamp >= cycle.start_time,
                            CycleAlarmLog.timestamp <= end_timestamp,
                        )
                        .values(cycle_id=cycle_id)
                    )

        task = asyncio.create_task(
            self.trigger_pdf_generation(cycle_id),
            name=f"pdf-report-cycle-{cycle_id}",
        )
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def trigger_pdf_generation(self, cycle_id: int) -> None:
        """
        Execute the asynchronous PDF generation & OS printing pipeline when a cycle
        completes (or run a lightweight placeholder when `persist_to_db=False`).
        """
        try:
            if self.config.persist_to_db and self.config.auto_generate_pdf:
                from app.report_generator import generate_cycle_pdf_report

                logger.info(
                    "[PDF Pipeline] Starting report generation for cycle_id=%d...",
                    cycle_id,
                )
                result = await generate_cycle_pdf_report(cycle_id=cycle_id)
                self.completed_cycle_ids.append(cycle_id)
                logger.info(
                    "[PDF Pipeline] Completed PDF report for cycle_id=%d -> %s (status=%s)",
                    cycle_id,
                    result.pdf_path.name,
                    result.status.value,
                )
            else:
                logger.info(
                    "[PDF Pipeline Placeholder] Starting report generation for cycle_id=%d...",
                    cycle_id,
                )
                await asyncio.sleep(self.config.pdf_simulation_delay)
                self.completed_cycle_ids.append(cycle_id)
                logger.info(
                    "[PDF Pipeline Placeholder] Report generation completed for cycle_id=%d.",
                    cycle_id,
                )
        except Exception as exc:
            logger.exception(
                "Failed to generate PDF report for cycle_id=%d: %s",
                cycle_id,
                exc,
            )
