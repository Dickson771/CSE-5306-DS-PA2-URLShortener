"""Quick local demos for consensus components."""
import asyncio
from typing import Dict

import typer

from .two_pc_node import TwoPCNode

app = typer.Typer(help="Local consensus demos")


async def _run_two_pc_demo(abort_participant: bool) -> None:
    """Run a short-lived 2PC flow with one coordinator and two participants."""
    host = "127.0.0.1"
    coordinator = TwoPCNode(
        node_id="coordinator",
        host=host,
        port=6100,
        phase="coordinator",
        peers={"p1": f"{host}:6101", "p2": f"{host}:6102"},
        abort_vote=False,
    )
    participants: Dict[str, TwoPCNode] = {
        "p1": TwoPCNode("p1", host, 6101, "participant", {}, abort_participant),
        "p2": TwoPCNode("p2", host, 6102, "participant", {}, False),
    }

    await asyncio.gather(coordinator.start(), *(p.start() for p in participants.values()))
    await asyncio.sleep(0.2)
    await coordinator.coordinate_transaction("demo", delay=0)
    await asyncio.sleep(0.2)
    await asyncio.gather(coordinator.stop(), *(p.stop() for p in participants.values()))


@app.command()
def two_pc_local(abort_participant: bool = typer.Option(False, help="Force participant p1 to vote abort")) -> None:
    """Run a short in-process 2PC demo (3 nodes) on localhost ports 6100-6102."""
    asyncio.run(_run_two_pc_demo(abort_participant))


if __name__ == "__main__":
    app()
