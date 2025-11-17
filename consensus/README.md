# Consensus Extensions (Assignment 3)

This folder contains standalone demos for the 2PC and Raft requirements from Assignment 3. Both flows use gRPC for all node-to-node traffic and ship with Docker Compose examples that spin up five containers to exercise the behaviors.

## Two-Phase Commit

- Proto: [`proto/two_phase_commit.proto`](proto/two_phase_commit.proto)
- Entrypoint: `python -m consensus.two_pc_node run`
- Logging format follows the assignment requirement: `Phase <phase> of Node <node_id> sends RPC <rpc_name> to Phase <phase> of Node <node_id>.`

Example (one coordinator, four participants):

```bash
python -m consensus.two_pc_node run \
  --node-id coordinator --phase coordinator --port 6100 \
  --peer n1=localhost:6101 --peer n2=localhost:6102 --peer n3=localhost:6103 --peer n4=localhost:6104 \
  --auto-start
```

Participants:

```bash
python -m consensus.two_pc_node run --node-id n1 --phase participant --port 6101
python -m consensus.two_pc_node run --node-id n2 --phase participant --port 6102
python -m consensus.two_pc_node run --node-id n3 --phase participant --port 6103
python -m consensus.two_pc_node run --node-id n4 --phase participant --port 6104
```

To launch the five containers automatically, run:

```bash
docker compose -f consensus/docker-compose.two_pc.yaml up
```

Set `--abort-vote` on any participant to force the coordinator to issue a global abort.

## Raft

- Proto: [`proto/raft.proto`](proto/raft.proto)
- Entrypoint: `python -m consensus.raft_node run`
- RPC logging format: `Node <node_id> sends RPC <rpc_name> to Node <node_id>.` and server side `Node <node_id> runs RPC <rpc_name> called by Node <node_id>.`
- Election timeout is randomized in `[1.5, 3.0]` seconds; heartbeats are sent every second by the current leader.

Example invocation for a single node with two peers:

```bash
python -m consensus.raft_node run \
  --node-id 1 --port 7101 \
  --peer 2=localhost:7102 --peer 3=localhost:7103
```

Any node can receive a client command via the `ClientCommand` RPC; followers forward the request to the latest known leader. The leader appends the command to its log, broadcasts the full log on the next heartbeat, waits for a majority of ACKs, and advances the commit index.

To spin up five containers with a full cluster:

```bash
docker compose -f consensus/docker-compose.raft.yaml up
```

## Dependencies

Install consensus dependencies locally with:

```bash
pip install -r consensus/requirements.txt
```
