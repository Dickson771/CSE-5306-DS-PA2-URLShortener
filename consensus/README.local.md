# Local Run & Test Guide (Consensus demos)

This guide walks through running and smoke-testing the Assignment 3 consensus demos—Two-Phase Commit (2PC) and Raft—on your local machine without Docker.

## Prerequisites
- Python 3.11 available on PATH
- `pip` for installing dependencies
- Optional: `grpcurl` (for ad-hoc RPCs) and `watch`/`tmux` for tailing multiple logs

Install the Python dependencies once:

```bash
pip install -r consensus/requirements.txt
```

## Two-Phase Commit (Q1 & Q2)

The 2PC demo uses one coordinator and four participants. Start each node in its own terminal so you can watch the required RPC log messages.

1) Start participants (ports 6101–6104):

```bash
python -m consensus.two_pc_node run --node-id n1 --phase participant --port 6101
python -m consensus.two_pc_node run --node-id n2 --phase participant --port 6102
python -m consensus.two_pc_node run --node-id n3 --phase participant --port 6103
python -m consensus.two_pc_node run --node-id n4 --phase participant --port 6104
```

2) Start the coordinator (port 6100) with peers defined. `--auto-start` kicks off a transaction immediately; omit it to trigger manually via the Typer prompt.

```bash
python -m consensus.two_pc_node run \
  --node-id coordinator --phase coordinator --port 6100 \
  --peer n1=localhost:6101 --peer n2=localhost:6102 --peer n3=localhost:6103 --peer n4=localhost:6104 \
  --auto-start
```

3) Observe logs. Client-side messages should follow `Phase <phase> of Node <node_id> sends RPC <rpc_name> to Phase <phase> of Node <node_id>.` Server-side messages should follow `Phase <phase> of Node <node_id> runs RPC <rpc_name> called by Phase <phase> of Node <node_id>.`

4) Force an abort path by adding `--abort-vote` to any participant. The coordinator will collect the abort and broadcast `Finalize(commit=false)` to all nodes.

### Quick verification loop
- Run `--auto-start` to execute a transaction.
- Confirm each participant prints the `Finalize` call with the expected decision (commit vs abort).

## Raft (Q3 & Q4)

The Raft demo uses five nodes. Election timeout is randomized between 1.5–3.0s; the leader sends 1s heartbeats. Start each node in its own terminal for readability.

Example ports: 7101–7105.

```bash
python -m consensus.raft_node run --node-id 1 --port 7101 --peer 2=localhost:7102 --peer 3=localhost:7103 --peer 4=localhost:7104 --peer 5=localhost:7105
python -m consensus.raft_node run --node-id 2 --port 7102 --peer 1=localhost:7101 --peer 3=localhost:7103 --peer 4=localhost:7104 --peer 5=localhost:7105
python -m consensus.raft_node run --node-id 3 --port 7103 --peer 1=localhost:7101 --peer 2=localhost:7102 --peer 4=localhost:7104 --peer 5=localhost:7105
python -m consensus.raft_node run --node-id 4 --port 7104 --peer 1=localhost:7101 --peer 2=localhost:7102 --peer 3=localhost:7103 --peer 5=localhost:7105
python -m consensus.raft_node run --node-id 5 --port 7105 --peer 1=localhost:7101 --peer 2=localhost:7102 --peer 3=localhost:7103 --peer 4=localhost:7104
```

### Observing leader election
- Within ~3 seconds a leader should announce itself via heartbeat `AppendEntries` RPCs.
- RPC logs use the formats `Node <node_id> sends RPC <rpc_name> to Node <node_id>.` (client) and `Node <node_id> runs RPC <rpc_name> called by Node <node_id>.` (server).

### Sending client commands (log replication)
Followers forward client requests to the leader automatically. You can trigger a client command from any node using the Typer prompt (press Enter at the prompt and type a command), or by issuing a gRPC request (e.g., with `grpcurl`). The leader appends the command to its log, broadcasts its full log on the next heartbeat, and advances the commit index after a majority of ACKs.

Example `grpcurl` against node 1 (assuming it is leader):

```bash
grpcurl -plaintext -d '{"command": "set x=1"}' localhost:7101 raft.Raft/ClientCommand
```

Confirm followers print `AppendEntries` runs with the updated log, and the leader advances its `commit_index` after ACKs.

## Docker Compose equivalents
If you prefer containers, the same flows run under Docker Compose:

```bash
docker compose -f consensus/docker-compose.two_pc.yaml up
# and/or
docker compose -f consensus/docker-compose.raft.yaml up
```

The compose files mount the repo to `/app`, install `consensus/requirements.txt`, and run the appropriate Typer command for each node.

## Cleanup
Terminate nodes with Ctrl+C in each terminal. For Docker Compose runs, use `docker compose -f <file> down` to stop and clean up containers and networks.
