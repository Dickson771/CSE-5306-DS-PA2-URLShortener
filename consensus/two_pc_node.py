import asyncio
import uuid
from typing import Dict, List

import grpc
import typer
from google.protobuf import empty_pb2

from consensus import two_phase_commit_pb2, two_phase_commit_pb2_grpc


def log_client(phase: str, node_id: str, rpc_name: str, target_phase: str, target_id: str) -> None:
    print(
        f"Phase {phase} of Node {node_id} sends RPC {rpc_name} to Phase {target_phase} of Node {target_id}."
    )


def log_server(phase: str, node_id: str, rpc_name: str, caller_phase: str, caller_id: str) -> None:
    print(
        f"Phase {phase} of Node {node_id} runs RPC {rpc_name} called by Phase {caller_phase} of Node {caller_id}."
    )


class TwoPhaseCommitService(two_phase_commit_pb2_grpc.TwoPhaseCommitServicer):
    def __init__(self, node: "TwoPCNode") -> None:
        self.node = node

    async def RequestVote(self, request: two_phase_commit_pb2.VoteRequest, context: grpc.aio.ServicerContext) -> two_phase_commit_pb2.VoteReply:  # type: ignore[override]
        log_server(self.node.phase, self.node.node_id, "RequestVote", request.coordinator_phase, request.coordinator_node_id)
        commit = self.node.should_commit(request)
        return two_phase_commit_pb2.VoteReply(
            commit=commit, participant_phase=self.node.phase, participant_node_id=self.node.node_id
        )

    async def Finalize(self, request: two_phase_commit_pb2.Decision, context: grpc.aio.ServicerContext) -> empty_pb2.Empty:  # type: ignore[override]
        log_server(self.node.phase, self.node.node_id, "Finalize", request.coordinator_phase, request.coordinator_node_id)
        self.node.finalize(request.commit)
        return empty_pb2.Empty()


class TwoPCNode:
    def __init__(self, node_id: str, host: str, port: int, phase: str, peers: Dict[str, str], abort_vote: bool):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.phase = phase
        self.peers = peers
        self.abort_vote = abort_vote
        self._server: grpc.aio.Server | None = None
        self._completed_transactions: List[str] = []

    def should_commit(self, request: two_phase_commit_pb2.VoteRequest) -> bool:
        if self.abort_vote:
            return False
        if request.payload:
            return True
        return True

    def finalize(self, commit: bool) -> None:
        status = "COMMIT" if commit else "ABORT"
        print(f"Node {self.node_id} finalized transaction with status: {status}")
        self._completed_transactions.append(status)

    async def start(self) -> None:
        self._server = grpc.aio.server()
        two_phase_commit_pb2_grpc.add_TwoPhaseCommitServicer_to_server(TwoPhaseCommitService(self), self._server)
        listen_addr = f"{self.host}:{self.port}"
        self._server.add_insecure_port(listen_addr)
        await self._server.start()
        print(f"TwoPC {self.phase} node {self.node_id} listening on {listen_addr}")

    async def stop(self) -> None:
        if self._server is None:
            return
        await self._server.stop(grace=None)

    async def _call_vote(self, peer_id: str, address: str, transaction_id: str, payload: str) -> bool:
        log_client(self.phase, self.node_id, "RequestVote", "participant", peer_id)
        async with grpc.aio.insecure_channel(address) as channel:
            stub = two_phase_commit_pb2_grpc.TwoPhaseCommitStub(channel)
            response = await stub.RequestVote(
                two_phase_commit_pb2.VoteRequest(
                    transaction_id=transaction_id,
                    payload=payload,
                    coordinator_phase=self.phase,
                    coordinator_node_id=self.node_id,
                )
            )
            return response.commit

    async def _send_decision(self, peer_id: str, address: str, transaction_id: str, commit: bool) -> None:
        log_client(self.phase, self.node_id, "Finalize", "participant", peer_id)
        async with grpc.aio.insecure_channel(address) as channel:
            stub = two_phase_commit_pb2_grpc.TwoPhaseCommitStub(channel)
            await stub.Finalize(
                two_phase_commit_pb2.Decision(
                    transaction_id=transaction_id,
                    commit=commit,
                    coordinator_phase=self.phase,
                    coordinator_node_id=self.node_id,
                )
            )

    async def coordinate_transaction(self, payload: str, delay: float = 0.5) -> None:
        if self.phase != "coordinator":
            print("Only coordinator nodes can start transactions")
            return
        transaction_id = str(uuid.uuid4())
        await asyncio.sleep(delay)
        votes = []
        for peer_id, address in self.peers.items():
            votes.append(await self._call_vote(peer_id, address, transaction_id, payload))
        commit = all(votes)
        await asyncio.gather(
            *[
                self._send_decision(peer_id, address, transaction_id, commit)
                for peer_id, address in self.peers.items()
            ]
        )
        self.finalize(commit)


app = typer.Typer(help="Two-Phase Commit demo node")


@app.command()
async def run(
    node_id: str = typer.Option(..., help="Unique node identifier"),
    host: str = typer.Option("0.0.0.0", help="Bind host"),
    port: int = typer.Option(6000, help="Bind port"),
    phase: str = typer.Option("participant", help="Phase name: coordinator or participant"),
    peer: List[str] = typer.Option([], help="Participant nodes in the form id=host:port"),
    payload: str = typer.Option("demo", help="Payload to vote on"),
    abort_vote: bool = typer.Option(False, help="Force this participant to vote abort"),
    auto_start: bool = typer.Option(False, help="Automatically start a transaction when coordinator"),
) -> None:
    peers: Dict[str, str] = {}
    for peer_entry in peer:
        peer_id, address = peer_entry.split("=", maxsplit=1)
        peers[peer_id] = address
    node = TwoPCNode(node_id, host, port, phase, peers, abort_vote)
    await node.start()
    if phase == "coordinator" and auto_start:
        await node.coordinate_transaction(payload)
    await asyncio.Event().wait()


if __name__ == "__main__":
    typer.run(run)
