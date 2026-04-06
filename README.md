# Distributed Fault-Tolerant Messaging System

A fully working distributed messaging system built in Java 11, covering
all four assignment areas: fault tolerance, data replication, time
synchronisation, and consensus.

---

## Team Members

| Member ID | Name                   | Email | Area | Package |
|-----------|------------------------|-------|------|---------|
| IT24610782 | M. A. V. E. Munasinghe | mavimukthieranga@gmail.com | Fault Tolerance | `com.messaging.fault` |
| IT24103551 | Sathushan Muguntthan   | jhcsathu09@gmail.com | Data Replication & Storage | `com.messaging.replication`, `com.messaging.storage` |
| IT24103696 | K. Varun               | kaneswaranvarun@gmail.com | Time Synchronisation | `com.messaging.timesync` |
| IT24103581 | Lalanga J. A. L.       | jayasinghe.lalanga88@gmail.com | Consensus (Raft) | `com.messaging.consensus.raft` |
---

## Features

### Fault Tolerance (IT24610782)
- **HeartbeatManager** — sends periodic heartbeats to all peers
- **FailureDetector** — declares a node dead after 3× missed heartbeat intervals; fires `onFailure` / `onRecovery` callbacks
- **FailoverManager** — reacts to failures; marks replicas offline so replication skips them
- **RecoveryManager** — when a node restarts it requests missing messages from the leader via `STATE_TRANSFER_REQUEST`

### Data Replication (IT24103551)
- **ReplicationManager** — replicates each message to `replicationFactor - 1` live peers concurrently; evaluates quorum based on the configured `consistencyModel` (`QUORUM` | `STRONG` | `EVENTUAL`)
- **QuorumManager** — tracks cluster membership and live node count
- **MessageStore** — thread-safe persistent store backed by JSON files; `Deduplicator` prevents double-processing across retries and failovers

### Time Synchronisation (IT24103696)
- **TimeSyncManager** — implements Cristian's Algorithm against a random peer every 60 s; maintains a smoothed `offset` using an exponential moving average (α = 0.25)
- **TimestampCorrector** — corrects client timestamps that exceed `MAX_CLOCK_SKEW_MS` (5 s); records per-sender offsets
- **MessageReorderer** — buffers incoming messages in a `PriorityBlockingQueue` per recipient and drains them in server-timestamp order after a 1-second reorder window

### Consensus (IT24103581)
- **RaftNode** — full Raft implementation (§5.1–5.4, §5.8):
    - Randomised election timeouts to prevent split votes
    - `RequestVote` / `VoteResponse` RPCs over real TCP
    - `AppendEntries` / `AppendResponse` RPCs for log replication and heartbeats
    - Log consistency check (prevLogIndex / prevLogTerm)
    - Commit index advancement once a majority acknowledges
    - No-op entry on leader election (§8)
- **RaftState** — persists `currentTerm` and `votedFor` to disk so they survive crashes
- **LeaderElection** — majority calculation and randomised timeout helper

---

## Prerequisites

- Java 11+
- Maven 3.6+

---

## Build

```bash
mvn clean package
```

The fat JAR is produced at `target/messaging-system.jar`.

---

## Running

### Option A — Shell scripts (Linux / macOS / Git Bash on Windows)

### 1. Start the cluster (3 nodes)

```bash
chmod +x scripts/*.sh
./scripts/start-cluster.sh
```

Each server reads its config from `config/server{1,2,3}.json`.
Logs are written to `logs/server{1,2,3}.log`.

### 2. Start a client

```bash
./scripts/start-client.sh
```

Follow the interactive menu to send messages and check your inbox.

### 3. Simulate a failure

```bash
# Kill server 2
./scripts/simulate-failure.sh 2

# Kill server 2, then restart it after 3 s
./scripts/simulate-failure.sh 2 --restart
```

### 4. Stop the cluster

```bash
./scripts/stop-cluster.sh
```
---
### Option B — Manual commands (all platforms including Windows CMD)

Use this if the scripts fail or you are on Windows without Git Bash.

**Start each server in a separate terminal window:**

```bash
# Terminal 1 — server 1
java -jar target/messaging-system.jar server config/server1.json
 
# Terminal 2 — server 2
java -jar target/messaging-system.jar server config/server2.json
 
# Terminal 3 — server 3
java -jar target/messaging-system.jar server config/server3.json
```

**Start the client in a fourth terminal:**

```bash
java -jar target/messaging-system.jar client
```

**Stop a node manually:** press `Ctrl+C` in its terminal window.
The shutdown hook will cleanly close all connections before exiting.

---
## Client Menu

Once the client connects you will see an interactive prompt:

```
╔══════════════════════════════════════╗
║  Distributed Messaging System Client ║
╚══════════════════════════════════════╝
Server host [localhost]: localhost
Server port [5001]: 5001
Connected as client-a1b2c3d4
 
─────────────────────────
 1. Send message
 2. Check my messages
 3. Show my client ID
 4. Exit
Choice:
```

| Option | What it does |
|--------|--------------|
| **1. Send message** | Prompts for a recipient client ID and message text, then delivers it through the cluster |
| **2. Check my messages** | Fetches your inbox from the server (default: last 10 messages) |
| **3. Show my client ID** | Displays the auto-generated ID assigned to your session — share this with others so they can message you |
| **4. Exit** | Disconnects cleanly |

> **Note:** The recipient must be the full client ID shown by option 3,
> e.g. `client-a1b2c3d4`. Open a second client in another terminal to
> get a second ID to test messaging between clients.
---
## Expected Log Output

When the cluster starts successfully, you should see lines like the following
in `logs/server1.log` (or in the terminal if running manually):

```
INFO  [MessagingServer]     ======== Starting server: server1 ========
INFO  [MessageStore]        MessageStore ready at data/server1 (0 messages)
INFO  [ConnectionManager]   ConnectionManager listening on port 5001
INFO  [TimeSyncManager]     [server1] TimeSyncManager started
INFO  [RaftNode]            [server1] RaftNode started
INFO  [ReplicationManager]  [server1] ReplicationManager started
INFO  [HeartbeatManager]    [server1] HeartbeatManager started (interval=500ms)
INFO  [FailureDetector]     [server1] FailureDetector started (threshold=1500ms)
INFO  [FailoverManager]     FailoverManager started
INFO  [RecoveryManager]     [server1] RecoveryManager started
```

Within ~2 seconds of all three nodes starting, one node will win the
Raft election. Look for this in any node's log to confirm the cluster
is healthy:

```
INFO  [RaftState]  [server2] Role transition CANDIDATE -> LEADER
INFO  [RaftNode]   [server2] Became LEADER for term 1
```

If you do **not** see a leader elected within 5 seconds, check the
troubleshooting section below.
---
## Configuration

Edit `config/server{N}.json`:

| Field | Default | Description |
|-------|---------|-------------|
| `serverId` | — | Unique node identifier |
| `port` | 5001–5003 | TCP port |
| `peers` | — | List of other nodes |
| `heartbeatInterval` | 500 ms | Heartbeat send interval |
| `electionTimeout` | 1500 ms | Raft election base timeout |
| `replicationFactor` | 3 | Number of copies to maintain |
| `consistencyModel` | QUORUM | `QUORUM`, `STRONG`, or `EVENTUAL` |
| `enableTimeSync` | true | Enable Cristian's Algorithm |
| `dataDir` | data | Root directory for persisted data |

---

## Testing

```bash
# All tests (unit + integration)
mvn test

# Unit tests only
mvn test -Dtest="com.messaging.unit.**"

# Integration tests only
mvn test -Dtest="com.messaging.integration.**"
```

Key test classes:

| Test | Covers |
|------|--------|
| `RaftNodeTest` | Election, log append, term management |
| `FailureDetectorTest` | Liveness detection, callbacks |
| `MessageStoreTest` | Store, dedup, persistence |
| `TimeSyncTest` | Timestamp correction, message reordering |
| `ClusterTest` | 3-node leader election |
| `FailoverTest` | Leader failure and re-election |

---
## Troubleshooting

**"Address already in use" on startup**

A previous server process is still holding the port. Find and kill it:

```bash
# Linux / macOS
lsof -i :5001 | grep LISTEN
kill -9 <PID>
 
# Windows
netstat -ano | findstr :5001
taskkill /PID <PID> /F
```

Or simply change the `port` value in the relevant `config/server{N}.json`
to a free port and update the matching entry in the other nodes' `peers` list.
 
---

**No leader elected after 5+ seconds**

- Confirm all three servers started without errors (`grep ERROR logs/*.log`).
- Check that the `port` and `peers` values in each config file are consistent
  and the ports are not blocked by a firewall.
- Try increasing `electionTimeout` in all three config files to `3000` if the
  machine is under heavy load.

---

**Client says "Connection failed"**

- Make sure at least one server is running before launching the client.
- Enter the correct host and port when prompted (default: `localhost` / `5001`).
- If you changed server ports in the config, enter the updated port at the
  client prompt.

---

**JAR not found**

Run `mvn clean package` from inside the `messaging/` directory (where
`pom.xml` lives) before starting any server or client.

---
## Architecture

```
Client
  │  (TCP / ObjectStream)
  ▼
ConnectionManager ──► dispatch()
                          │
              ┌───────────┴────────────┐
              ▼                        ▼
  ClientRequestHandler        PeerRequestHandler
              │                        │
              ▼               ┌────────┼────────┐
       MessageService      RaftNode  Replication  Fault/TimeSync
              │
    ┌─────────┼──────────┐
    ▼         ▼          ▼
MessageStore  Raft   ReplicationManager
```

---

## GitHub Commits


Suggested per-member branch strategy:

- `feature/fault-tolerance` — IT24610782
- `feature/replication`     — IT24103551
- `feature/time-sync`       — IT24103696
- `feature/consensus`       — IT24103581
