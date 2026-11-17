"""
Asynchronous OPC-UA Client & 0.5s State-Machine Polling Engine for Siemens S7-1500.

Implements a deterministic 3-state machine:
- State 1 (IDLE): Polls the boolean `Cycle_Active` trigger node every 0.5s.
- State 2 (ACTIVE): Executes single-request bulk reads (`client.read_values()`)
  for all configured data tags every 0.5s and logs them to the database.
- State 3 (COMPLETION): Triggered on the falling edge (TRUE -> FALSE) of
  `Cycle_Active`, finalizing the cycle record and dispatching PDF report generation
  asynchronously before returning immediately to IDLE.
"""

import asyncio
import enum
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from asyncua import Client, Node
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


class S7CyclePoller:
    """
    High-efficiency OPC-UA polling client for a Siemens S7-1500 PLC.

    Uses monotonic drift-compensated timing to maintain a strict 0.5-second
    polling cadence and performs single-request bulk reads (`client.read_values`)
    so network round-trip latency never accumulates across tags.
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

        # Active cycle tracking
        self._active_cycle_id: Optional[int] = None

    def _prepare_node_references(self, client: Client) -> None:
        """
        Pre-resolve `asyncua.Node` objects for the trigger tag and all configured
        process data tags so no node-resolution overhead occurs inside the 0.5s loop.
        """
        self._trigger_node = client.get_node(self.config.trigger_node_id)
        self._tag_names = list(self.config.data_nodes_map.keys())
        self._data_nodes = [
            client.get_node(node_id_str)
            for node_id_str in self.config.data_nodes_map.values()
        ]
        logger.info(
            "Prepared OPC-UA node handles: trigger='%s', bulk data tags=%d",
            self.config.trigger_node_id,
            len(self._data_nodes),
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
                    logger.info("Connected to S7-1500 OPC-UA server at %s", self.endpoint_url)
                    self._prepare_node_references(client)
                    await self._run_state_machine_loop(client)

            except (UaError, OSError, asyncio.TimeoutError) as exc:
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
                        "Transition IDLE -> ACTIVE (cycle_id=%s). Executing initial bulk read.",
                        self._active_cycle_id,
                    )
                    # Immediately capture the t=0 sample upon rising edge
                    await self._poll_active_cycle_step(client)

            elif self.state == PollerState.ACTIVE:
                # ---------------------------------------------------------
                # STATE 2 (ACTIVE): Bulk-read trigger + all data tags
                # in a SINGLE OPC-UA Read service request.
                # ---------------------------------------------------------
                await self._poll_active_cycle_step(client)

            elif self.state == PollerState.COMPLETION:
                # ---------------------------------------------------------
                # STATE 3 (COMPLETION): Finalize cycle & dispatch PDF job
                # ---------------------------------------------------------
                completed_cycle_id = self._active_cycle_id
                self._active_cycle_id = None
                self.state = PollerState.IDLE

                if completed_cycle_id is not None:
                    await self._on_cycle_complete(completed_cycle_id)

                logger.info("Transition COMPLETION -> IDLE. Monitoring for next cycle.")

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

    async def _poll_active_cycle_step(self, client: Client) -> None:
        """
        Perform a single-request bulk read of `[trigger_node, *data_nodes]`.
         Note: `client.read_values(nodes)` (alias `client.read_nodes` in asyncua)
        batches all nodes into one OPC-UA ReadRequest packet.
        """
        assert self._trigger_node is not None
        assert self._active_cycle_id is not None

        # Combine trigger node and all process data nodes into a single bulk read
        all_nodes: list[Node] = [self._trigger_node, *self._data_nodes]
        sample_timestamp = datetime.now(timezone.utc)

        # Single network round-trip to S7-1500
        bulk_read_fn = getattr(client, "read_nodes", None) or client.read_values
        values: list[Any] = await bulk_read_fn(all_nodes)

        cycle_active = bool(values[0])
        tag_values = values[1:]

        if not cycle_active:
            # Falling edge detected (TRUE -> FALSE) -> move to COMPLETION
            logger.info(
                "Falling edge detected on Cycle_Active for cycle_id=%s -> State COMPLETION",
                self._active_cycle_id,
            )
            self.state = PollerState.COMPLETION
            return

        # Package tag readings and log to database
        readings: dict[str, Any] = dict(zip(self._tag_names, tag_values))
        await self.log_timeseries_batch(
            cycle_id=self._active_cycle_id,
            timestamp=sample_timestamp,
            readings=readings,
        )

    # =====================================================================
    # Database & PDF Trigger Functions (Scaffold / Placeholders)
    # =====================================================================

    async def _on_cycle_start(self) -> int:
        """
        Create a new `CycleRecord` row in the database when `Cycle_Active` goes TRUE.
        """
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

        Uses SQLAlchemy Core `insert()` for minimal ORM overhead during
        high-frequency 2 Hz polling.
        """
        if not readings:
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

        logger.debug(
            "Logged %d tag values for cycle_id=%d at %s",
            len(rows_to_insert),
            cycle_id,
            timestamp.isoformat(),
        )

    async def _on_cycle_complete(self, cycle_id: int) -> None:
        """
        Mark the `CycleRecord` as COMPLETED with its `end_time` and spawn an
        asynchronous background task to generate and print the PDF report so
        the OPC-UA polling loop is never blocked.
        """
        end_timestamp = datetime.now(timezone.utc)
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
        Placeholder coroutine for the PDF generation & OS printing pipeline.

        In the full implementation, this will:
        1. Query `CycleRecord`, `ReportSchema`, and `CycleTimeSeries` rows.
        2. Build a Pandas DataFrame and render Matplotlib charts to temp PNGs.
        3. Render the Jinja2 HTML template and convert to PDF via WeasyPrint.
        4. Dispatch the PDF to `win32print` and update `CycleRecord.status`.
        """
        logger.info(
            "[PDF Pipeline Placeholder] Starting report generation for cycle_id=%d...",
            cycle_id,
        )
        try:
            # Simulate non-blocking report compilation work
            await asyncio.sleep(0.1)
            logger.info(
                "[PDF Pipeline Placeholder] Report generation queued/completed for cycle_id=%d.",
                cycle_id,
            )
        except Exception as exc:
            logger.exception(
                "Failed to generate PDF report for cycle_id=%d: %s",
                cycle_id,
                exc,
            )


# =========================================================================
# Standalone Example / Smoke-Test Entrypoint
# =========================================================================
async def _example_main() -> None:
    """Example wiring showing how `S7CyclePoller` is configured and launched."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    sample_config = CyclePollerConfig(
        report_schema_id=1,
        trigger_node_id='ns=3;s="Cycle_Control"."Cycle_Active"',
        data_nodes_map={
            "Temp_Zone1": 'ns=3;s="Cycle_Data"."Temp_Zone1"',
            "Temp_Zone2": 'ns=3;s="Cycle_Data"."Temp_Zone2"',
            "Pressure_Main": 'ns=3;s="Cycle_Data"."Pressure_Main"',
            "Motor_Speed_RPM": 'ns=3;s="Cycle_Data"."Motor_Speed_RPM"',
        },
        poll_interval=0.5,
    )

    poller = S7CyclePoller(
        endpoint_url="opc.tcp://192.168.0.1:4840",
        config=sample_config,
    )
    await poller.start()


if __name__ == "__main__":
    asyncio.run(_example_main())
