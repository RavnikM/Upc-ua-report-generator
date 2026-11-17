"""
Simulated Siemens S7-1500 OPC-UA Server for local testing.

Exposes exact S7-1500 Data Block string NodeIDs in Namespace 3 (`ns=3;s=...`):
- `ns=3;s="Cycle_Control"."Cycle_Active"` (Boolean)
- `ns=3;s="Cycle_Data"."Temp_Zone1"` (Double / LReal)
- `ns=3;s="Cycle_Data"."Temp_Zone2"` (Double / LReal)
- `ns=3;s="Cycle_Data"."Pressure_Main"` (Double / LReal)
- `ns=3;s="Cycle_Data"."Motor_Speed_RPM"` (Double / LReal)
- `ns=3;s="Cycle_Data"."Step_Counter"` (Int32 / DInt)
- `ns=3;s="Cycle_Data"."Alarm_OverTemp"` (Boolean / Bool)
- `ns=3;s="Cycle_Data"."Recipe_Name"` (String)
"""

import asyncio
import logging
import math
from typing import Any, Optional

from asyncua import Node, Server, ua

logger = logging.getLogger(__name__)

DEFAULT_MOCK_ENDPOINT = "opc.tcp://127.0.0.1:48401"


class MockS71500Server:
    """
    In-process asyncua Server emulating a Siemens S7-1500 PLC OPC-UA interface.
    """

    def __init__(
        self,
        endpoint: str = DEFAULT_MOCK_ENDPOINT,
        initial_cycle_active: bool = False,
    ) -> None:
        self.endpoint = endpoint
        self.initial_cycle_active = initial_cycle_active
        self.server = Server()
        self.nodes: dict[str, Node] = {}
        self._running = False

    async def init_server(self) -> None:
        """Initialize the OPC-UA address space with S7-1500 namespace (ns=3) and DB tags."""
        await self.server.init()
        self.server.set_endpoint(self.endpoint)
        self.server.set_server_name("SIMATIC S7-1500 OPC-UA Simulator")

        # Register namespaces so SIMATIC S7 namespace lands on ns=3
        await self.server.register_namespace("urn:siemens:s7:internal")
        ns_s7 = await self.server.register_namespace(
            "http://www.siemens.com/simatic-s7-opcua"
        )
        assert ns_s7 == 3, f"Expected S7 namespace index 3, got {ns_s7}"

        objects = self.server.nodes.objects
        plc_folder = await objects.add_folder(ns_s7, "PLC_1")

        # 1. Trigger Node: ns=3;s="Cycle_Control"."Cycle_Active"
        trigger_node_id = ua.NodeId('"Cycle_Control"."Cycle_Active"', ns_s7)
        trigger_node = await plc_folder.add_variable(
            trigger_node_id,
            "Cycle_Active",
            self.initial_cycle_active,
            varianttype=ua.VariantType.Boolean,
        )
        await trigger_node.set_writable()
        self.nodes["Cycle_Active"] = trigger_node

        # 2. Floating-Point Process Data Nodes in "Cycle_Data" DB
        float_tags: dict[str, float] = {
            "Temp_Zone1": 180.0,
            "Temp_Zone2": 195.0,
            "Pressure_Main": 4.2,
            "Motor_Speed_RPM": 1450.0,
        }

        for tag_name, init_val in float_tags.items():
            s7_node_id = ua.NodeId(f'"Cycle_Data"."{tag_name}"', ns_s7)
            var_node = await plc_folder.add_variable(
                s7_node_id,
                tag_name,
                float(init_val),
                varianttype=ua.VariantType.Double,
            )
            await var_node.set_writable()
            self.nodes[tag_name] = var_node

        # 3. Additional S7-1500 Data Block types (Int32, Boolean, String)
        mixed_tags: list[tuple[str, Any, ua.VariantType]] = [
            ("Step_Counter", 1, ua.VariantType.Int32),
            ("Alarm_OverTemp", False, ua.VariantType.Boolean),
            ("Recipe_Name", "RECIPE_A_200C", ua.VariantType.String),
        ]
        for tag_name, init_val, vtype in mixed_tags:
            s7_node_id = ua.NodeId(f'"Cycle_Data"."{tag_name}"', ns_s7)
            var_node = await plc_folder.add_variable(
                s7_node_id,
                tag_name,
                init_val,
                varianttype=vtype,
            )
            await var_node.set_writable()
            self.nodes[tag_name] = var_node

        logger.info(
            "Initialized Mock S7-1500 OPC-UA Server at %s (ns=%d, %d nodes, initial_active=%s)",
            self.endpoint,
            ns_s7,
            len(self.nodes),
            self.initial_cycle_active,
        )

    async def set_cycle_active(self, active: bool) -> None:
        """Toggle `ns=3;s="Cycle_Control"."Cycle_Active"` on the simulated PLC."""
        await self.nodes["Cycle_Active"].write_value(
            ua.Variant(active, ua.VariantType.Boolean)
        )
        logger.info("[PLC SIM] Set Cycle_Active = %s", active)

    async def update_process_values(
        self,
        step: int,
        alarm_overtemp: bool = False,
        recipe_name: str = "RECIPE_A_200C",
    ) -> dict[str, Any]:
        """
        Update the simulated S7-1500 DB tags with realistic curves for a given step.
        """
        float_updates = {
            "Temp_Zone1": round(180.0 + 2.5 * step + math.sin(step) * 0.8, 2),
            "Temp_Zone2": round(195.0 + 1.8 * step + math.cos(step) * 0.6, 2),
            "Pressure_Main": round(4.5 + 0.35 * math.sin(step * 0.7), 2),
            "Motor_Speed_RPM": round(1450.0 + (15.0 * step), 1),
        }
        for tag_name, val in float_updates.items():
            await self.nodes[tag_name].write_value(
                ua.Variant(val, ua.VariantType.Double)
            )

        await self.nodes["Step_Counter"].write_value(
            ua.Variant(int(step), ua.VariantType.Int32)
        )
        await self.nodes["Alarm_OverTemp"].write_value(
            ua.Variant(bool(alarm_overtemp), ua.VariantType.Boolean)
        )
        await self.nodes["Recipe_Name"].write_value(
            ua.Variant(str(recipe_name), ua.VariantType.String)
        )

        return {
            **float_updates,
            "Step_Counter": step,
            "Alarm_OverTemp": alarm_overtemp,
            "Recipe_Name": recipe_name,
        }

    async def __aenter__(self) -> "MockS71500Server":
        await self.init_server()
        await self.server.start()
        self._running = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._running:
            await self.server.stop()
            self._running = False


async def run_standalone_simulator(duration_seconds: Optional[float] = None) -> None:
    """Run the simulated S7-1500 server standalone with periodic cycle toggling."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )
    async with MockS71500Server() as plc:
        logger.info("Standalone S7-1500 simulator running on %s", plc.endpoint)
        elapsed = 0.0
        step = 0
        while duration_seconds is None or elapsed < duration_seconds:
            await plc.set_cycle_active(False)
            await asyncio.sleep(2.0)
            await plc.set_cycle_active(True)
            for _ in range(6):
                step += 1
                await plc.update_process_values(step)
                await asyncio.sleep(0.5)
            elapsed += 5.0


if __name__ == "__main__":
    asyncio.run(run_standalone_simulator())
