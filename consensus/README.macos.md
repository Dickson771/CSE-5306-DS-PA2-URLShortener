# Running the consensus demos locally on an Apple Silicon (M2) Mac

This guide assumes a fresh macOS environment with no developer tools installed. It covers installing dependencies, creating a Python 3.11 virtual environment, running the Python-only demos, and using Docker Compose to launch five-node clusters for the 2PC and Raft assignments.

## 1. Install prerequisites

1) Install the Xcode Command Line Tools (compilers and headers used by Python packages):

```bash
xcode-select --install
```

2) Install Homebrew (package manager). If you already have it, skip this step:

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

3) Install Python 3.11 and supporting tools:

```bash
brew install python@3.11 git
```

4) Install Docker Desktop for Mac (Apple Silicon build) from https://www.docker.com/products/docker-desktop/. After installation, start Docker Desktop so that containers can run.

## 2. Clone the repository

```bash
cd ~
git clone <your-fork-or-repo-url> CSE-5306-DS-PA2-URLShortener
cd CSE-5306-DS-PA2-URLShortener
```

## 3. Create and activate a virtual environment

macOS ships with an older `pip`, so create an isolated environment tied to Python 3.11:

```bash
/opt/homebrew/bin/python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

## 4. Install Python dependencies

```bash
python -m pip install -r consensus/requirements.txt
```

## 5. Run the two-phase commit demo (Python-only)

Start four participant nodes and one coordinator in separate shells (or `tmux` panes). Replace `<node-id>` with unique IDs like `p1`–`p4` for participants and `coord` for the coordinator.

Participants (example for `p1`):

```bash
python -m consensus.two_pc_node run \
  --node-id p1 \
  --phase participant \
  --host 127.0.0.1 --port 50051 \
  --coordinator-host 127.0.0.1 --coordinator-port 50050
```

Coordinator (after participants are listening):

```bash
python -m consensus.two_pc_node run \
  --node-id coord \
  --phase coordinator \
  --host 127.0.0.1 --port 50050 \
  --participant 127.0.0.1:50051 \
  --participant 127.0.0.1:50052 \
  --participant 127.0.0.1:50053 \
  --participant 127.0.0.1:50054
```

The coordinator will issue vote requests and finalize commit/abort based on participant responses. Logs follow the assignment-required formats.

## 6. Run the Raft demo (Python-only)

Start five nodes on distinct ports. Every node needs the full peer list. The example below uses `n1`–`n5`.

```bash
# Terminal 1
python -m consensus.raft_node run --node-id n1 --host 127.0.0.1 --port 6001 \
  --peer 127.0.0.1:6002 --peer 127.0.0.1:6003 --peer 127.0.0.1:6004 --peer 127.0.0.1:6005

# Terminal 2
python -m consensus.raft_node run --node-id n2 --host 127.0.0.1 --port 6002 \
  --peer 127.0.0.1:6001 --peer 127.0.0.1:6003 --peer 127.0.0.1:6004 --peer 127.0.0.1:6005

# Repeat similarly for n3, n4, n5 on ports 6003–6005 with all peers listed.
```

To submit a client command (e.g., `set x=1`) against the current leader, use any node’s listening address:

```bash
grpcurl -plaintext -d '{"command": "set x=1"}' localhost:6001 raft.Raft/ClientCommand
```

Followers will forward the request to the leader. The leader appends the entry, replicates the full log, and advances the commit index after a majority of ACKs.

## 7. Run with Docker Compose (five-node clusters)

Make sure Docker Desktop is running, then from the repository root:

### Two-Phase Commit

```bash
docker compose -f consensus/docker-compose.two_pc.yaml up --build
```

This brings up one coordinator and four participant containers on a shared network. Stop with `Ctrl+C` and remove containers with `docker compose -f consensus/docker-compose.two_pc.yaml down`.

### Raft

```bash
docker compose -f consensus/docker-compose.raft.yaml up --build
```

This launches five Raft nodes with randomized election timeouts and 1-second heartbeats. Stop with `Ctrl+C` and clean up with `docker compose -f consensus/docker-compose.raft.yaml down`.

## 8. Troubleshooting on Apple Silicon

- If `grpcio` fails to build wheels, ensure Rosetta 2 is installed (`softwareupdate --install-rosetta --agree-to-license`) and retry after reactivating the virtual environment.
- If ports are already in use, pick different `--port` values and update peer lists accordingly.
- If Docker Compose reports architecture issues, confirm Docker Desktop is updated to the latest Apple Silicon release.

You now have a clean setup path to run and test the consensus demos locally on an M2 Mac.
