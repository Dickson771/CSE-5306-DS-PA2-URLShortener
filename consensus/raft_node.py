import asyncio
import random
from dataclasses import dataclass
from typing import Dict, List, Optional

import grpc
import typer

from . import raft_pb2, raft_pb2_grpc


STATE_FOLLOWER = "follower"
STATE_CANDIDATE = "candidate"
STATE_LEADER = "leader"


@dataclass
class LogEntry:
    term: int
    index: int
    command: str


class RaftService(raft_pb2_grpc.RaftServicer):
    def __init__(self, node: "RaftNode") -> None:
        self.node = node

    async def RequestVote(self, request: raft_pb2.RequestVoteRequest, context: grpc.aio.ServicerContext) -> raft_pb2.RequestVoteResponse:  # type: ignore[override]
        print(f"Node {self.node.node_id} runs RPC RequestVote called by Node {request.candidate_id}.")
        return await self.node.handle_vote_request(request)

    async def AppendEntries(self, request: raft_pb2.AppendEntriesRequest, context: grpc.aio.ServicerContext) -> raft_pb2.AppendEntriesResponse:  # type: ignore[override]
        print(f"Node {self.node.node_id} runs RPC AppendEntries called by Node {request.leader_id}.")
        return await self.node.handle_append_entries(request)

    async def ClientCommand(self, request: raft_pb2.ClientCommandRequest, context: grpc.aio.ServicerContext) -> raft_pb2.ClientCommandResponse:  # type: ignore[override]
        print(f"Node {self.node.node_id} runs RPC ClientCommand called by Node {request.caller_id or self.node.node_id}.")
        return await self.node.handle_client_command(request)


class RaftNode:
    def __init__(self, node_id: int, host: str, port: int, peers: Dict[int, str]) -> None:
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers
        self.state = STATE_FOLLOWER
        self.current_term = 0
        self.voted_for: Optional[int] = None
        self.log: List[LogEntry] = []
        self.commit_index = 0
        self.leader_id: Optional[int] = None
        self._server: Optional[grpc.aio.Server] = None
        self._reset_election = asyncio.Event()
        self._heartbeat_task: Optional[asyncio.Task[None]] = None

    @property
    def last_log_index(self) -> int:
        if not self.log:
            return 0
        return self.log[-1].index

    @property
    def last_log_term(self) -> int:
        if not self.log:
            return 0
        return self.log[-1].term

    async def start(self) -> None:
        self._server = grpc.aio.server()
        raft_pb2_grpc.add_RaftServicer_to_server(RaftService(self), self._server)
        listen_addr = f"{self.host}:{self.port}"
        self._server.add_insecure_port(listen_addr)
        await self._server.start()
        print(f"Raft node {self.node_id} listening on {listen_addr}")
        asyncio.create_task(self._election_loop())

    async def stop(self) -> None:
        if self._server is None:
            return
        await self._server.stop(grace=None)

    async def _election_loop(self) -> None:
        while True:
            timeout = random.uniform(1.5, 3.0)
            try:
                self._reset_election.clear()
                await asyncio.wait_for(self._reset_election.wait(), timeout=timeout)
                continue
            except asyncio.TimeoutError:
                await self.start_election()

    async def start_election(self) -> None:
        self.state = STATE_CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        votes = 1
        print(f"Node {self.node_id} became candidate for term {self.current_term}")
        results = await asyncio.gather(
            *[self.request_vote(peer_id, address) for peer_id, address in self.peers.items()]
        )
        votes += sum(1 for granted in results if granted)
        if votes > (len(self.peers) + 1) // 2:
            await self.become_leader()
        else:
            self.state = STATE_FOLLOWER

    async def become_leader(self) -> None:
        self.state = STATE_LEADER
        self.leader_id = self.node_id
        print(f"Node {self.node_id} became leader for term {self.current_term}")
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self) -> None:
        while self.state == STATE_LEADER:
            await self.broadcast_append_entries()
            await asyncio.sleep(1.0)

    async def broadcast_append_entries(self) -> None:
        await asyncio.gather(
            *[
                self.send_append_entries(peer_id, address)
                for peer_id, address in self.peers.items()
            ]
        )

    async def request_vote(self, peer_id: int, address: str) -> bool:
        print(f"Node {self.node_id} sends RPC RequestVote to Node {peer_id}.")
        async with grpc.aio.insecure_channel(address) as channel:
            stub = raft_pb2_grpc.RaftStub(channel)
            response = await stub.RequestVote(
                raft_pb2.RequestVoteRequest(
                    term=self.current_term,
                    candidate_id=self.node_id,
                    last_log_index=self.last_log_index,
                    last_log_term=self.last_log_term,
                )
            )
        if response.term > self.current_term:
            self.current_term = response.term
            self.state = STATE_FOLLOWER
            self.voted_for = None
        return response.vote_granted

    async def send_append_entries(self, peer_id: int, address: str) -> None:
        print(f"Node {self.node_id} sends RPC AppendEntries to Node {peer_id}.")
        async with grpc.aio.insecure_channel(address) as channel:
            stub = raft_pb2_grpc.RaftStub(channel)
            entries = [
                raft_pb2.LogEntry(term=entry.term, index=entry.index, command=entry.command)
                for entry in self.log
            ]
            response = await stub.AppendEntries(
                raft_pb2.AppendEntriesRequest(
                    term=self.current_term,
                    leader_id=self.node_id,
                    entries=entries,
                    leader_commit=self.commit_index,
                )
            )
        if response.term > self.current_term:
            self.current_term = response.term
            self.state = STATE_FOLLOWER
            self.voted_for = None

    async def handle_vote_request(self, request: raft_pb2.RequestVoteRequest) -> raft_pb2.RequestVoteResponse:
        granted = False
        if request.term < self.current_term:
            granted = False
        else:
            if request.term > self.current_term:
                self.current_term = request.term
                self.voted_for = None
            log_ok = (request.last_log_term > self.last_log_term) or (
                request.last_log_term == self.last_log_term and request.last_log_index >= self.last_log_index
            )
            if log_ok and (self.voted_for is None or self.voted_for == request.candidate_id):
                self.voted_for = request.candidate_id
                granted = True
                self.state = STATE_FOLLOWER
                self._reset_election.set()
        return raft_pb2.RequestVoteResponse(term=self.current_term, vote_granted=granted)

    async def handle_append_entries(self, request: raft_pb2.AppendEntriesRequest) -> raft_pb2.AppendEntriesResponse:
        if request.term < self.current_term:
            return raft_pb2.AppendEntriesResponse(term=self.current_term, success=False, match_index=self.last_log_index)
        self.state = STATE_FOLLOWER
        self.leader_id = request.leader_id
        self.current_term = request.term
        self.log = [LogEntry(term=entry.term, index=entry.index, command=entry.command) for entry in request.entries]
        if request.leader_commit > self.commit_index:
            self.commit_index = min(request.leader_commit, len(self.log))
        self._reset_election.set()
        return raft_pb2.AppendEntriesResponse(term=self.current_term, success=True, match_index=self.last_log_index)

    async def handle_client_command(self, request: raft_pb2.ClientCommandRequest) -> raft_pb2.ClientCommandResponse:
        if self.state != STATE_LEADER:
            if self.leader_id and self.leader_id in self.peers:
                leader_address = self.peers[self.leader_id]
                caller_id = request.caller_id or self.node_id
                print(f"Node {self.node_id} sends RPC ClientCommand to Node {self.leader_id}.")
                async with grpc.aio.insecure_channel(leader_address) as channel:
                    stub = raft_pb2_grpc.RaftStub(channel)
                    response = await stub.ClientCommand(
                        raft_pb2.ClientCommandRequest(command=request.command, caller_id=caller_id)
                    )
                    return response
            return raft_pb2.ClientCommandResponse(accepted=False, leader_hint=str(self.leader_id or "unknown"), committed_index=self.commit_index)
        new_index = self.last_log_index + 1
        self.log.append(LogEntry(term=self.current_term, index=new_index, command=request.command))
        await self.broadcast_append_entries()
        ack_count = 1
        for peer_id, address in self.peers.items():
            async with grpc.aio.insecure_channel(address) as channel:
                stub = raft_pb2_grpc.RaftStub(channel)
                response = await stub.AppendEntries(
                    raft_pb2.AppendEntriesRequest(
                        term=self.current_term,
                        leader_id=self.node_id,
                        entries=[raft_pb2.LogEntry(term=entry.term, index=entry.index, command=entry.command) for entry in self.log],
                        leader_commit=self.commit_index,
                    )
                )
                if response.success:
                    ack_count += 1
        if ack_count > (len(self.peers) + 1) // 2:
            self.commit_index = new_index
        return raft_pb2.ClientCommandResponse(accepted=True, leader_hint=str(self.node_id), committed_index=self.commit_index)


app = typer.Typer(help="Simplified Raft node")


async def _run_node(node_id: int, host: str, port: int, peer: List[str]) -> None:
    peers: Dict[int, str] = {}
    for peer_entry in peer:
        peer_id_str, address = peer_entry.split("=", maxsplit=1)
        peers[int(peer_id_str)] = address
    node = RaftNode(node_id, host, port, peers)
    await node.start()
    await asyncio.Event().wait()


@app.command()
def run(
    node_id: int = typer.Option(..., help="Unique node identifier"),
    host: str = typer.Option("0.0.0.0", help="Bind host"),
    port: int = typer.Option(7000, help="Bind port"),
    peer: List[str] = typer.Option([], help="Peer nodes in the form id=host:port"),
) -> None:
    asyncio.run(_run_node(node_id=node_id, host=host, port=port, peer=peer))


if __name__ == "__main__":
    app()
