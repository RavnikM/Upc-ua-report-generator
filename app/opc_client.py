"""
Asynchronous OPC-UA Client & 0.5s State-Machine Polling Engine for Siemens S7-1500.

Implements a deterministic 3-state machine:
- State 1 (IDLE): Polls the boolean `Cycle_Active` trigger node every 0.5s.
- State 2 (ACTIVE): Executes single-request bulk reads (`client.read_values()` /
  `client.read_nodes()`) for all configured data tags every 0.5s and logs them.
- State 3 (COMPLETION): Triggered on the falling edge (TRUE -> FALSE) of
  `Cycle_Active`, finalizing the cycle record and dispatching PDF report generation
  asynchronously before returning immediately to IDLE.
"""

import asyncio
import enum
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
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
    # Mapping of logical tag_name -> OPC-UA NodeId string
    # e.g., {"Temp_Zone1": 'ns=3;s="Cycle_Data"."Temp_Zone1"'}
    data_nodes_map: dict[str, str]
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

    Uses monotonic drift-compensated timing to maintain a strict 0.5-second
    polling cadence and performs single-request bulk reads (`client.read_values` /
    `client.read_nodes`) so network round-trip latency never accumulates across tags.
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
        self._tag_names: list[str] = []
        self._data_nodes: list[Node] = []

        # Diagnostics & telemetry counters
        self.invalid_data_nodes: dict[str, str] = {}
        self.reconnect_count: int = 0
        self._has_connected_once: bool = False

        # Active cycle tracking
        self._active_cycle_id: Optional[int] = None
        self._in_memory_cycle_counter: int = 0
        self.recorded_batches: list[PolledSampleBatch] = []
        self.completed_cycle_ids: list[int] = []

    def _prepare_node_references(self, client: Client) -> None:
        """
        Pre-resolve `asyncua.Node` objects for the trigger tag and all configured
        process data tags so no node-resolution overhead occurs inside the 0.5s loop.
        Also ensures `client.read_nodes` is available as an alias to `client.read_values`
        in `asyncua >= 1.x / 2.x`.
        """
        if not hasattr(client, "read_nodes"):
            setattr(client, "read_nodes", client.read_values)

        self._trigger_node = client.get_node(self.config.trigger_node_id)
        self._tag_names = list(self.config.data_nodes_map.keys())
        self._data_nodes = [
            client.get_node(node_id_str)
            for node_id_str in self.config.data_nodes_map.values()
        ]
        logger.info(
            "Prepared OPC-UA node handles: trigger='%s', bulk data tags=%d (%s)",
            self.config.trigger_node_id,
            len(self._data_nodes),
            ", ".join(self._tag_names),
        )

    async def _validate_nodes(self, client: Client) -> None:
        """
        Perform a single bulk attribute read upon connection to verify that the
        trigger NodeId and all configured data NodeIds exist in the S7-1500 address space.

        - If the trigger node is invalid (e.g., `BadNodeIdUnknown`), raises `UaError`.
        - If any data node is invalid, logs a warning and records it in
          `self.invalid_data_nodes` without halting valid tags.
        """
        assert self._trigger_node is not None
        all_nodes: list[Node] = [self._trigger_node, *self._data_nodes]
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
        for tag_name, dv in zip(self._tag_names, data_values[1:]):
            if not dv.StatusCode.is_good():
                node_id_str = self.config.data_nodes_map[tag_name]
                self.invalid_data_nodes[tag_name] = dv.StatusCode.name
                logger.warning(
                    "Configured data tag '%s' (%s) returned non-good StatusCode: %s (%s)",
                    tag_name,
                    node_id_str,
                    dv.StatusCode.name,
                    dv.StatusCode.doc,
                )

    async def start(self) -> None:
        """
        Main supervision loop with automatic reconnection to the S7-1500 PLC.
        """
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
        """
        Execute the 0.5-second drift-compensated state machine loop.
        """
        assert self._trigger_node is not None, "Trigger node must be initialized."

        while not self._stop_event.is_set():
            loop_start_monotonic = time.monotonic()

            if self.state == PollerState.IDLE:
                # ---------------------------------------------------------
                # STATE 1 (IDLE): Poll boolean `Cycle_Active` tag
                # ---------------------------------------------------------
                cycle_active = bool(await self._trigger_node.read_value())

                if cycle_active:
                    self._active_cycle_id = await self._on_cycle_start()
                    self.state = PollerState.ACTIVE
                    logger.info(
                        "[STATE: IDLE -> ACTIVE] Rising edge on Cycle_Active! "
                        "Started cycle_id=%d. Executing t=0 bulk read.",
                        self._active_cycle_id,
                    )
                    # Immediately capture the t=0 sample upon rising edge
                    await self._poll_active_cycle_step(client)

                    # Handle edge case where trigger immediately drops within same tick
                    if self.state == PollerState.COMPLETION:
                        await self._handle_completion_transition()

            elif self.state == PollerState.ACTIVE:
                # ---------------------------------------------------------
                # STATE 2 (ACTIVE): Bulk-read trigger + all data tags
                # in a SINGLE OPC-UA Read service request.
                # ---------------------------------------------------------
                await self._poll_active_cycle_step(client)

                # If falling edge occurred during _poll_active_cycle_step,
                # immediately process State 3 (COMPLETION) in the same tick
                if self.state == PollerState.COMPLETION:
                    await self._handle_completion_transition()

            elif self.state == PollerState.COMPLETION:
                # ---------------------------------------------------------
                # STATE 3 (COMPLETION): Finalize cycle & dispatch PDF job
                # ---------------------------------------------------------
                await self._handle_completion_transition()

            # Drift-compensated sleep to keep cycle cadence at 0.5s
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
        self.state = PollerState.IDLE

        if completed_cycle_id is not None:
            await self._on_cycle_complete(completed_cycle_id)

        logger.info("[STATE: COMPLETION -> IDLE] Monitoring for next cycle.")

    async def _poll_active_cycle_step(self, client: Client) -> None:
        """
        Perform a single-request bulk read of `[trigger_node, *data_nodes]`.
        Calls `client.read_nodes(all_nodes)` (backed by `client.read_values`),
        which sends one OPC-UA ReadRequest packet for all nodes simultaneously.
        """
        assert self._trigger_node is not None
        assert self._active_cycle_id is not None

        # Combine trigger node and all process data nodes into a single bulk read
        all_nodes: list[Node] = [self._trigger_node, *self._data_nodes]
        sample_timestamp = datetime.now(timezone.utc)

        # Single network round-trip to S7-1500
        bulk_read_fn = getattr(client, "read_nodes", client.read_values)
        values: list[Any] = await bulk_read_fn(all_nodes)

        cycle_active = bool(values[0])
        tag_values = values[1:]

        if not cycle_active:
            # Falling edge detected (TRUE -> FALSE) -> move to COMPLETION
            logger.info(
                "[STATE: ACTIVE -> COMPLETION] Falling edge detected on Cycle_Active "
                "for cycle_id=%d.",
                self._active_cycle_id,
            )
            self.state = PollerState.COMPLETION
            return

        # Package tag readings and log
        readings: dict[str, Any] = dict(zip(self._tag_names, tag_values))
        await self.log_timeseries_batch(
            cycle_id=self._active_cycle_id,
            timestamp=sample_timestamp,
            readings=readings,
        )

    # =====================================================================
    # Database & PDF Trigger Functions
    # =====================================================================

    async def _on_cycle_start(self) -> int:
        """
        Create a new `CycleRecord` row in the database when `Cycle_Active` goes TRUE
        (or allocate an in-memory cycle ID if `persist_to_db=False`).
        """
        if not self.config.persist_to_db:
            self._in_memory_cycle_counter += 1
            return self._in_memory_cycle_counter

        async with get_Session_context() as session:
            new_cycle = CycleRecord(
                report_schema_id=self.config.report_schema_id,
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
        """
        Bulk-insert a 0.5-second snapshot of all polled tags into `CycleTimeSeries`.

        Also appends to `self.recorded_batches` for verification and diagnostics.
        """
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
        Mark the `CycleRecord` as COMPLETED with its `end_time` and spawn an
        asynchronous background task to generate and print the PDF report so
        the OPC-UA polling loop is never blocked.
        """
        end_timestamp = datetime.now(timezone.utc)
        if self.config.persist_to_db:
            async with get_Session_context() as session:
                cycle = await session.get(CycleRecord, cycle_id)
                if cycle is not None:
                    cycle.end_time = end_timestamp
                    cycle.status = CycleStatus.COMPLETED

        # Fire-and-forget background task with strong reference tracking
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
