"""
Comprehensive Stage 1 Verification Suite: OPC-UA Bulk Polling & State Machine.

Tests 6 industrial scenarios against `MockS71500Server` (with `persist_to_db=False`):
1. Standard Cycle Lifecycle (`IDLE -> ACTIVE -> COMPLETION -> IDLE`) & 0.5s cadence.
2. Back-to-Back Consecutive Cycles with Slow PDF Generation (non-blocking verification).
3. Hot-Start Recovery (poller boots while PLC already has `Cycle_Active == True`).
4. Mid-Cycle PLC Disconnect & Automatic Reconnection Recovery (resumes active cycle
   without splitting `cycle_id`, then completes cleanly).
5. Mixed S7-1500 Data Block Types (`LReal`, `DInt`, `Bool`, `String`) in a single bulk read.
6. Invalid NodeId Diagnostics (`BadNodeIdUnknown`) on connect without crashing valid tags.
"""

import asyncio
import logging
import sys
from pathlib import Path

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.opc_client import CyclePollerConfig, PollerState, S7CyclePoller
from tests.mock_s7_server import MockS71500Server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logging.getLogger("asyncua").setLevel(logging.WARNING)
logger = logging.getLogger("test_opc_scenarios")

STANDARD_DATA_NODES = {
    "Temp_Zone1": 'ns=3;s="Cycle_Data"."Temp_Zone1"',
    "Temp_Zone2": 'ns=3;s="Cycle_Data"."Temp_Zone2"',
    "Pressure_Main": 'ns=3;s="Cycle_Data"."Pressure_Main"',
    "Motor_Speed_RPM": 'ns=3;s="Cycle_Data"."Motor_Speed_RPM"',
}


async def scenario_1_standard_cycle() -> None:
    """Scenario 1: Standard IDLE -> ACTIVE -> COMPLETION -> IDLE & 0.5s cadence."""
    logger.info("=" * 72)
    logger.info("SCENARIO 1: Standard Cycle & 0.5s Bulk-Read Cadence")
    logger.info("=" * 72)
    endpoint = "opc.tcp://127.0.0.1:48411"

    config = CyclePollerConfig(
        report_schema_id=1,
        trigger_node_id='ns=3;s="Cycle_Control"."Cycle_Active"',
        data_nodes_map=STANDARD_DATA_NODES,
        poll_interval=0.5,
        persist_to_db=False,
    )

    async with MockS71500Server(endpoint=endpoint) as plc:
        poller = S7CyclePoller(endpoint_url=endpoint, config=config)
        task = asyncio.create_task(poller.start())
        try:
            await asyncio.sleep(0.8)
            assert poller.state == PollerState.IDLE
            assert len(poller.recorded_batches) == 0

            await plc.update_process_values(step=1)
            await plc.set_cycle_active(True)
            for step in range(2, 6):
                await asyncio.sleep(0.5)
                await plc.update_process_values(step=step)

            assert poller.state == PollerState.ACTIVE
            assert len(poller.recorded_batches) >= 4

            await plc.set_cycle_active(False)
            await asyncio.sleep(0.7)

            assert poller.state == PollerState.IDLE
            assert poller.completed_cycle_ids == [1]

            timestamps = [b.timestamp for b in poller.recorded_batches]
            intervals = [
                (t2 - t1).total_seconds()
                for t1, t2 in zip(timestamps[:-1], timestamps[1:])
            ]
            avg_dt = sum(intervals) / len(intervals)
            assert 0.45 <= avg_dt <= 0.55, f"Poll interval drifted: {avg_dt:.3f}s"
            logger.info(
                "-> Scenario 1 PASSED (batches=%d, avg_interval=%.3fs)",
                len(poller.recorded_batches),
                avg_dt,
            )
        finally:
            await poller.stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def scenario_2_back_to_back_cycles_non_blocking_pdf() -> None:
    """
    Scenario 2: Cycle 1 triggers a slow PDF build (1.2s), and Cycle 2 starts
    immediately (0.5s later) while Cycle 1's PDF is still rendering.
    Verifies that Cycle 2 is immediately detected and polled without delay.
    """
    logger.info("=" * 72)
    logger.info("SCENARIO 2: Back-to-Back Cycles with Slow (1.2s) PDF Generation")
    logger.info("=" * 72)
    endpoint = "opc.tcp://127.0.0.1:48412"

    config = CyclePollerConfig(
        report_schema_id=1,
        trigger_node_id='ns=3;s="Cycle_Control"."Cycle_Active"',
        data_nodes_map=STANDARD_DATA_NODES,
        poll_interval=0.5,
        persist_to_db=False,
        pdf_simulation_delay=1.2,  # Simulate heavy WeasyPrint + Matplotlib rendering
    )

    async with MockS71500Server(endpoint=endpoint) as plc:
        poller = S7CyclePoller(endpoint_url=endpoint, config=config)
        task = asyncio.create_task(poller.start())
        try:
            await asyncio.sleep(0.6)

            # --- Cycle 1 ---
            await plc.update_process_values(step=1)
            await plc.set_cycle_active(True)
            await asyncio.sleep(1.1)  # ~2-3 samples for Cycle 1
            await plc.set_cycle_active(False)

            # Wait 0.55s so poller detects falling edge of Cycle 1 and spawns 1.2s PDF job
            await asyncio.sleep(0.55)
            assert poller.state == PollerState.IDLE
            # Cycle 1 PDF is STILL running in the background (takes 1.2s)
            assert poller.completed_cycle_ids == [], "Cycle 1 PDF should still be running"

            # --- Immediately start Cycle 2 while Cycle 1 PDF is still rendering ---
            await plc.update_process_values(step=10)
            await plc.set_cycle_active(True)
            await asyncio.sleep(1.1)  # Poll Cycle 2 for ~2-3 ticks
            assert poller.state == PollerState.ACTIVE

            await plc.set_cycle_active(False)
            # Wait for Cycle 2 falling edge + PDF completion
            await asyncio.sleep(1.8)

            cycle_1_batches = [b for b in poller.recorded_batches if b.cycle_id == 1]
            cycle_2_batches = [b for b in poller.recorded_batches if b.cycle_id == 2]

            assert len(cycle_1_batches) >= 2, f"Cycle 1 batches: {len(cycle_1_batches)}"
            assert len(cycle_2_batches) >= 2, f"Cycle 2 batches: {len(cycle_2_batches)}"
            assert poller.completed_cycle_ids == [1, 2], (
                f"Both cycles should complete PDF generation: {poller.completed_cycle_ids}"
            )
            logger.info(
                "-> Scenario 2 PASSED (Cycle 1 batches=%d, Cycle 2 batches=%d, PDFs=%s)",
                len(cycle_1_batches),
                len(cycle_2_batches),
                poller.completed_cycle_ids,
            )
        finally:
            await poller.stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def scenario_3_hot_start_already_active() -> None:
    """
    Scenario 3: Poller starts up while the S7-1500 is ALREADY mid-cycle
    (`Cycle_Active == True` before the poller connects).
    """
    logger.info("=" * 72)
    logger.info("SCENARIO 3: Hot-Start Recovery (Cycle_Active=True on Startup)")
    logger.info("=" * 72)
    endpoint = "opc.tcp://127.0.0.1:48413"

    config = CyclePollerConfig(
        report_schema_id=1,
        trigger_node_id='ns=3;s="Cycle_Control"."Cycle_Active"',
        data_nodes_map=STANDARD_DATA_NODES,
        poll_interval=0.5,
        persist_to_db=False,
    )

    async with MockS71500Server(endpoint=endpoint, initial_cycle_active=True) as plc:
        await plc.update_process_values(step=5)
        poller = S7CyclePoller(endpoint_url=endpoint, config=config)
        task = asyncio.create_task(poller.start())
        try:
            # Within first tick after connecting, poller should be ACTIVE
            await asyncio.sleep(1.2)
            assert poller.state == PollerState.ACTIVE
            assert len(poller.recorded_batches) >= 2

            await plc.set_cycle_active(False)
            await asyncio.sleep(0.7)
            assert poller.state == PollerState.IDLE
            assert poller.completed_cycle_ids == [1]
            logger.info(
                "-> Scenario 3 PASSED (Hot-started into cycle_id=1, captured %d batches)",
                len(poller.recorded_batches),
            )
        finally:
            await poller.stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def scenario_4_mid_cycle_disconnect_and_reconnect() -> None:
    """
    Scenario 4: Mid-Cycle PLC Disconnect & Automatic Reconnection Recovery.
    - PLC starts Cycle 1 (`Cycle_Active=True`).
    - Poller captures initial samples.
    - PLC server drops offline for 1.0s while Cycle 1 is ACTIVE.
    - PLC server restarts on the same endpoint with `Cycle_Active=True`.
    - Poller automatically reconnects, resumes logging to the SAME `cycle_id=1`,
      and then completes `cycle_id=1` when `Cycle_Active` goes `False`.
    """
    logger.info("=" * 72)
    logger.info("SCENARIO 4: Mid-Cycle PLC Disconnect & Auto-Reconnect Recovery")
    logger.info("=" * 72)
    endpoint = "opc.tcp://127.0.0.1:48414"

    config = CyclePollerConfig(
        report_schema_id=1,
        trigger_node_id='ns=3;s="Cycle_Control"."Cycle_Active"',
        data_nodes_map=STANDARD_DATA_NODES,
        poll_interval=0.5,
        reconnect_delay=0.5,  # Fast reconnect for testing
        persist_to_db=False,
    )

    poller = S7CyclePoller(endpoint_url=endpoint, config=config)
    task = asyncio.create_task(poller.start())

    try:
        # 1. Start PLC instance #1 and begin an active cycle
        async with MockS71500Server(endpoint=endpoint, initial_cycle_active=True) as plc1:
            await plc1.update_process_values(step=1)
            await asyncio.sleep(1.2)
            batches_before_drop = len(poller.recorded_batches)
            assert poller.state == PollerState.ACTIVE
            assert batches_before_drop >= 2
            logger.info(
                "Simulating sudden PLC network outage after %d batches...",
                batches_before_drop,
            )

        # PLC server is now stopped (simulating network loss / PLC reboot)
        await asyncio.sleep(0.8)

        # 2. Bring PLC instance #2 back online on the same endpoint with Cycle_Active=True
        async with MockS71500Server(endpoint=endpoint, initial_cycle_active=True) as plc2:
            await plc2.update_process_values(step=20)
            await asyncio.sleep(1.3)

            assert poller.reconnect_count >= 1, "Poller should have recorded a reconnect"
            assert poller.state == PollerState.ACTIVE
            batches_after_reconnect = len(poller.recorded_batches)
            assert batches_after_reconnect > batches_before_drop, (
                "Poller should resume logging batches after reconnect"
            )
            # All batches before and after outage must belong to the same cycle_id=1
            cycle_ids = {b.cycle_id for b in poller.recorded_batches}
            assert cycle_ids == {1}, f"Expected all batches on cycle_id=1, got {cycle_ids}"

            # 3. End the cycle cleanly
            await plc2.set_cycle_active(False)
            await asyncio.sleep(0.7)
            assert poller.state == PollerState.IDLE
            assert poller.completed_cycle_ids == [1]

            logger.info(
                "-> Scenario 4 PASSED (reconnects=%d, batches_before=%d, total_batches=%d, cycle_ids=%s)",
                poller.reconnect_count,
                batches_before_drop,
                batches_after_reconnect,
                cycle_ids,
            )
    finally:
        await poller.stop()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def scenario_5_mixed_s7_datatypes_and_invalid_node_diagnostics() -> None:
    """
    Scenario 5 & 6:
    - Reads mixed S7-1500 Data Block types (`Double`, `Int32`, `Boolean`, `String`)
      in a single bulk request.
    - Also includes one deliberately non-existent tag (`ns=3;s="Cycle_Data"."Ghost_Tag"`)
      to verify that `_validate_nodes` flags `BadNodeIdUnknown` in `poller.invalid_data_nodes`
      while continuing to poll all valid tags without crashing.
    """
    logger.info("=" * 72)
    logger.info("SCENARIO 5 & 6: Mixed S7 Data Types + BadNodeIdUnknown Diagnostics")
    logger.info("=" * 72)
    endpoint = "opc.tcp://127.0.0.1:48415"

    mixed_nodes = {
        **STANDARD_DATA_NODES,
        "Step_Counter": 'ns=3;s="Cycle_Data"."Step_Counter"',
        "Alarm_OverTemp": 'ns=3;s="Cycle_Data"."Alarm_OverTemp"',
        "Recipe_Name": 'ns=3;s="Cycle_Data"."Recipe_Name"',
        "Ghost_Tag": 'ns=3;s="Cycle_Data"."Ghost_Tag"',  # Does not exist on PLC
    }

    config = CyclePollerConfig(
        report_schema_id=1,
        trigger_node_id='ns=3;s="Cycle_Control"."Cycle_Active"',
        data_nodes_map=mixed_nodes,
        poll_interval=0.5,
        persist_to_db=False,
    )

    async with MockS71500Server(endpoint=endpoint) as plc:
        poller = S7CyclePoller(endpoint_url=endpoint, config=config)
        task = asyncio.create_task(poller.start())
        try:
            await asyncio.sleep(0.6)
            # Verify that Ghost_Tag was flagged during connection node validation
            assert "Ghost_Tag" in poller.invalid_data_nodes, (
                f"Expected Ghost_Tag in invalid_data_nodes, got {poller.invalid_data_nodes}"
            )
            assert poller.invalid_data_nodes["Ghost_Tag"] == "BadNodeIdUnknown"

            # Trigger active cycle with mixed types (Step=42, Alarm=True, Recipe="CURING_CYCLE_B")
            await plc.update_process_values(
                step=42,
                alarm_overtemp=True,
                recipe_name="CURING_CYCLE_B",
            )
            await plc.set_cycle_active(True)
            await asyncio.sleep(0.7)
            await plc.set_cycle_active(False)
            await asyncio.sleep(0.7)

            assert len(poller.recorded_batches) >= 1
            sample = poller.recorded_batches[0].readings
            assert isinstance(sample["Temp_Zone1"], float)
            assert sample["Step_Counter"] == 42 and isinstance(sample["Step_Counter"], int)
            assert sample["Alarm_OverTemp"] is True
            assert sample["Recipe_Name"] == "CURING_CYCLE_B"
            assert sample["Ghost_Tag"] is None

            logger.info(
                "-> Scenario 5 & 6 PASSED (Invalid node flagged=%s, Mixed readings=%s)",
                poller.invalid_data_nodes,
                sample,
            )
        finally:
            await poller.stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def run_all_opc_scenarios() -> None:
    """Execute all Stage 1 OPC-UA verification scenarios sequentially."""
    await scenario_1_standard_cycle()
    await scenario_2_back_to_back_cycles_non_blocking_pdf()
    await scenario_3_hot_start_already_active()
    await scenario_4_mid_cycle_disconnect_and_reconnect()
    await scenario_5_mixed_s7_datatypes_and_invalid_node_diagnostics()

    logger.info("=" * 72)
    logger.info("ALL 6 STAGE-1 OPC-UA SCENARIOS PASSED SUCCESSFULLY!")
    logger.info("=" * 72)


if __name__ == "__main__":
    asyncio.run(run_all_opc_scenarios())
