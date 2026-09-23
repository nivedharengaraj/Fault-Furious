# Fault & Furious: Week 1 --- Sharded Telemetry Store

**Project:** Fault & Furious --- Distributed Incident Triage Swarm\
**Team:** DataStorm\
**Milestone:** Week 1 --- Sharded Store

**Goal for this week:** Build a small distributed telemetry store that
partitions telemetry across nodes, replicates each shard three times,
acknowledges writes using a quorum, and rebalances data when nodes join
or leave.

> **Scope boundary:** Week 1 is only the data layer. Do not implement
> LLM calls, agents, gossip, CRDT belief states, Raft, incident
> diagnosis, or the final UI yet.

------------------------------------------------------------------------

## 0. What are we building?

The final project will eventually look like:

``` text
                 INCIDENT
                    |
                    v
          +-------------------+
          | Distributed Store |  <-- Week 1
          +---------+---------+
                    |
             partial evidence
                    |
          +---------+---------+
          v         v         v
       Agent A   Agent B   Agent C
          |         |         |
          +---- gossip/CRDT --+
                    |
                    v
                Diagnosis
```

This week we build the box in the middle.

For every telemetry record, the system must:

1.  Determine its logical shard.
2.  Use consistent hashing to select the responsible nodes.
3.  Store the shard on **three distinct replicas**.
4.  Treat a write as successful only after a **quorum of 2/3 replicas**
    acknowledges it.
5.  Handle a node joining the cluster.
6.  Handle a node leaving the cluster.
7.  Rebalance affected shards without losing logical records.
8.  Provide a simple interface that later agents can use to request
    telemetry.

### Week 1 deliverable

> **A working distributed telemetry store with sharding, three-way
> replication, quorum writes, node membership changes, and
> rebalancing.**

------------------------------------------------------------------------

# 1. What data are we storing?

Do not connect to a real production system yet. Generate synthetic
telemetry.

A record should contain at least:

``` json
{
  "event_id": "evt-000123",
  "timestamp": "2026-09-23T10:15:32Z",
  "service": "checkout",
  "host": "checkout-03",
  "type": "metric",
  "name": "request_latency_ms",
  "value": 842,
  "severity": "warning",
  "message": "Request latency above threshold"
}
```

Use these telemetry types:

-   `metric`
-   `log`
-   `trace`
-   `deployment`
-   `config`

At minimum every record needs:

-   `event_id`
-   `timestamp`
-   `service`
-   `type`
-   `payload` or value

------------------------------------------------------------------------

# 2. First-time setup

Everyone should first get the project running locally.

Install:

-   Python 3.10+
-   Git
-   Docker
-   Docker Compose

Check:

``` bash
python3 --version
git --version
docker --version
docker compose version
```

Create the environment:

``` bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

For the HTTP-based prototype, install:

``` bash
pip install fastapi uvicorn pydantic pytest httpx
pip freeze > requirements.txt
```

> If the team chooses another transport library, agree on it once and
> update this README. Do not let each person implement a different
> transport layer.

------------------------------------------------------------------------

# 3. Repository structure

Create:

``` text
fault-and-furious/
├── README.md
├── docs/
│   └── week-1-sharded-store.md
├── src/
│   └── storage/
│       ├── __init__.py
│       ├── models.py
│       ├── hash_ring.py
│       ├── node.py
│       ├── replica_manager.py
│       ├── quorum.py
│       ├── rebalance.py
│       └── store.py
├── scripts/
│   ├── generate_telemetry.py
│   ├── start_cluster.py
│   └── verify_cluster.py
├── tests/
│   ├── test_hash_ring.py
│   ├── test_replication.py
│   ├── test_quorum.py
│   ├── test_rebalancing.py
│   └── test_end_to_end.py
├── docker/
│   └── Dockerfile
├── data/
│   └── generated/
├── docker-compose.yml
├── requirements.txt
└── .gitignore
```

Add generated data, local node state, `.venv`, and secrets to
`.gitignore`.

------------------------------------------------------------------------

# 4. Understand the four concepts before coding

Do not mix these concepts together.

``` text
SHARDING
"Where should this data live?"

        ↓

REPLICATION
"Which 3 nodes keep a copy?"

        ↓

QUORUM
"How many replicas must acknowledge a write?"

        ↓

REBALANCING
"What changes when cluster membership changes?"
```

------------------------------------------------------------------------

# 5. Step 1 --- Define the telemetry model

Create:

``` text
src/storage/models.py
```

Define a `TelemetryRecord`.

Conceptually:

``` text
TelemetryRecord
├── event_id
├── timestamp
├── service
├── host
├── type
└── payload
```

Also define the logical shard key.

The proposal partitions telemetry primarily by **service and time
window**.

For example:

``` text
checkout|2026-09-23T10:00
checkout|2026-09-23T11:00
payments|2026-09-23T10:00
```

Do not use the raw event timestamp as the complete key, because that
would create an effectively unique shard key for every event.

Use a fixed time bucket such as one hour for the first implementation.

------------------------------------------------------------------------

# 6. Step 2 --- Generate synthetic telemetry

Create:

``` text
scripts/generate_telemetry.py
```

Start with:

``` text
10,000 records
```

across:

``` text
checkout
payments
inventory
search
users
recommendations
```

Use all five telemetry types:

``` text
metric
log
trace
deployment
config
```

Use a fixed random seed for tests:

``` python
random.seed(42)
```

The generated file can be:

``` text
data/generated/telemetry.jsonl
```

JSON Lines is convenient because one record occupies one line.

Example:

``` text
{"event_id":"evt-1", ...}
{"event_id":"evt-2", ...}
{"event_id":"evt-3", ...}
```

Verify the count:

``` bash
wc -l data/generated/telemetry.jsonl
```

Expected:

``` text
10000
```

------------------------------------------------------------------------

# 7. Step 3 --- Implement consistent hashing

Create:

``` text
src/storage/hash_ring.py
```

The ring determines which physical nodes are responsible for a shard.

Do **not** start with:

``` python
hash(key) % number_of_nodes
```

because adding or removing a node can remap a very large portion of the
dataset.

Instead:

1.  Hash every node into a circular keyspace.
2.  Add multiple virtual positions for each physical node.
3.  Hash each shard key into the same keyspace.
4.  Walk clockwise to find the first responsible node.

Use:

``` text
100 virtual nodes / physical node
```

as the initial configuration.

Example:

``` text
node-01 → node-01-0 ... node-01-99
node-02 → node-02-0 ... node-02-99
node-03 → node-03-0 ... node-03-99
```

Make this configurable.

------------------------------------------------------------------------

# 8. Required hash-ring API

Implement a class with operations equivalent to:

``` python
add_node(node_id)
remove_node(node_id)
get_node(key)
get_replica_nodes(key, replication_factor=3)
```

Example:

``` python
ring.get_node("checkout|2026-09-23T10:00")
```

returns one primary/owner.

And:

``` python
ring.get_replica_nodes(
    "checkout|2026-09-23T10:00",
    replication_factor=3
)
```

returns three **different physical nodes**.

Example:

``` text
[
    "node-03",
    "node-01",
    "node-05"
]
```

Do not count different virtual nodes belonging to the same physical node
as separate replicas.

------------------------------------------------------------------------

# 9. Step 4 --- Test hashing before building storage

Create:

``` text
tests/test_hash_ring.py
```

### Test A --- deterministic ownership

The same key and same ring membership must always produce the same
owner.

### Test B --- distribution

Generate thousands of keys and verify that multiple nodes receive
assignments.

### Test C --- three distinct replicas

For every tested key:

``` text
replica_count == 3
unique_physical_nodes == 3
```

### Test D --- node join

Measure how many keys change ownership when:

``` text
A B C E
```

becomes:

``` text
A B C D E
```

The point is not to hit one exact percentage. The point is to show that
a small portion of the keyspace moves instead of almost everything
moving.

### Test E --- node leave

Repeat after removing one node.

------------------------------------------------------------------------

# 10. Step 5 --- Build a storage node

Create:

``` text
src/storage/node.py
```

Each storage node needs:

``` text
node_id
address
local storage
```

For the first prototype, local storage can simply be a dictionary or
small local persistence layer.

Conceptually:

``` python
{
    "checkout|2026-09-23T10:00": [
        record_1,
        record_2
    ]
}
```

Do not optimize for database performance in Week 1.

The purpose is to prove the distributed-storage behavior.

------------------------------------------------------------------------

# 11. Node API

Every node should expose at least:

``` text
PUT /records
GET /records/{shard_key}
GET /health
```

Useful optional endpoints:

``` text
GET /shards
POST /replicate
DELETE /shards/{shard_key}
```

A health response can be:

``` json
{
  "node_id": "node-03",
  "status": "healthy"
}
```

This endpoint will later help with failure injection.

------------------------------------------------------------------------

# 12. Step 6 --- Build the distributed store

Create:

``` text
src/storage/store.py
```

The client should talk to the `DistributedStore`, not directly to
physical nodes.

Conceptually:

``` text
Client
  |
  | PUT telemetry
  v
DistributedStore
  |
  +-- calculate shard key
  |
  +-- consult hash ring
  |
  +-- select 3 replicas
  |
  +-- send writes
       |
       +-- Node A
       +-- Node B
       +-- Node C
```

The distributed store owns:

-   hash ring
-   node membership
-   replication factor
-   write quorum
-   routing

------------------------------------------------------------------------

# 13. Step 7 --- Implement three-way replication

Set:

``` text
REPLICATION_FACTOR = 3
```

For every shard, select:

``` text
Replica 1
Replica 2
Replica 3
```

They must be three distinct physical nodes.

Example:

``` text
Shard A
  ├── node-03
  ├── node-01
  └── node-05
```

When a record is written, the distributed store sends the record to all
three replicas.

------------------------------------------------------------------------

# 14. Step 8 --- Implement quorum acknowledgement

Use:

``` text
Replication factor = 3
Write quorum = 2
```

A write is successful when at least two of the three replicas
acknowledge it.

Example:

``` text
Node A → ACK
Node B → ACK
Node C → timeout

2/3

WRITE = SUCCESS
```

But:

``` text
Node A → timeout
Node B → timeout
Node C → ACK

1/3

WRITE = FAILURE
```

Return enough information to debug the result:

``` json
{
  "success": true,
  "acks": 2,
  "required": 2,
  "replicas": [
    "node-01",
    "node-04",
    "node-07"
  ]
}
```

------------------------------------------------------------------------

# 15. Step 9 --- Test quorum under failure

This test is mandatory.

### Case 1

``` text
A ✓
B ✓
C ✗
```

Expected:

``` text
2/3 → SUCCESS
```

### Case 2

``` text
A ✗
B ✗
C ✓
```

Expected:

``` text
1/3 → FAILURE
```

### Case 3

``` text
A ✓
B ✓
C ✓
```

Expected:

``` text
3/3 → SUCCESS
```

This demonstrates that the system does not require every replica to be
available, but also does not accept a write with only one
acknowledgement.

------------------------------------------------------------------------

# 16. Step 10 --- Define cluster membership

For the first prototype, keep membership explicit.

Example:

``` yaml
nodes:
  - node-01
  - node-02
  - node-03
  - node-04
  - node-05
```

The membership list is used to construct the hash ring.

Later milestones can introduce more sophisticated membership/failure
detection.

Week 1 only needs to demonstrate:

``` text
ADD NODE
REMOVE NODE
```

------------------------------------------------------------------------

# 17. Step 11 --- Implement node join

Start with:

``` text
node-01
node-02
node-03
node-04
node-05
```

Ingest the 10,000 records.

Record the current ownership and replica placement.

Then add:

``` text
node-06
```

The hash ring changes.

The system must determine which shards now have different replica
assignments.

Do NOT copy every shard to every node.

Only affected shard placements should move.

------------------------------------------------------------------------

# 18. Step 12 --- Implement rebalancing

Create:

``` text
src/storage/rebalance.py
```

Rebalancing compares:

``` text
OLD RING
```

with:

``` text
NEW RING
```

For each affected shard:

1.  Determine old replicas.
2.  Determine new replicas.
3.  Identify which new replicas are missing the data.
4.  Copy the shard to the required new replica.
5.  Verify the copied data.
6.  Remove obsolete copies only after the new copy is safe.
7.  Record the new placement.

Example:

``` text
BEFORE

Shard A
├── node-01
├── node-03
└── node-05

node-06 joins

AFTER

Shard A
├── node-01
├── node-05
└── node-06
```

The exact affected shards will depend on the hash ring.

------------------------------------------------------------------------

# 19. Step 13 --- Implement node leave

Start with:

``` text
A B C D E
```

Remove:

``` text
C
```

The rebalancer must:

1.  Remove C from membership.
2.  Find shards for which C was a replica.
3.  Find replacement nodes.
4.  Copy the affected data.
5.  Restore three replicas wherever the cluster has at least three
    healthy nodes.
6.  Verify the data.
7.  Remove obsolete references to C.

After rebalancing:

``` text
Every healthy shard
        ↓
3 active replicas
```

should hold true.

------------------------------------------------------------------------

# 20. Critical invariant

After a node join or leave:

``` text
active replicas per shard == 3
```

for all shards, provided the cluster has at least three active nodes.

This is one of the most important assertions in the entire milestone.

------------------------------------------------------------------------

# 21. Step 14 --- Verify logical data after rebalancing

Do not only verify that the nodes are running.

Before rebalancing:

``` text
logical records = 10,000
```

After node join:

``` text
logical records = 10,000
```

After node leave:

``` text
logical records = 10,000
```

Replication creates multiple physical copies, but the number of
**logical records** must not change.

------------------------------------------------------------------------

# 22. Step 15 --- Build a verification script

Create:

``` text
scripts/verify_cluster.py
```

It should eventually print something similar to:

``` text
Cluster verification
--------------------

Active nodes: 6

Logical records: 10,000

Replication:
  Shards: 120
  Target replicas/shard: 3
  Healthy shards: 120/120

Quorum:
  Write quorum: 2
  Successful test writes: 100/100

Rebalancing:
  Shards moved: 31
  Logical records lost: 0

STATUS: PASS
```

The exact number of shards depends on your service/time-window
bucketing.

------------------------------------------------------------------------

# 23. Step 16 --- End-to-end test

Create:

``` text
tests/test_end_to_end.py
```

Run this sequence:

``` text
START CLUSTER
      ↓
GENERATE 10,000 RECORDS
      ↓
INGEST
      ↓
VERIFY 3 REPLICAS
      ↓
REMOVE NODE
      ↓
REBALANCE
      ↓
VERIFY DATA
      ↓
ADD NODE
      ↓
REBALANCE
      ↓
VERIFY DATA AGAIN
      ↓
TEST QUORUM FAILURE
```

The complete scenario should be repeatable.

------------------------------------------------------------------------

# 24. Minimum acceptance scenario

Use:

``` text
5 nodes
10,000 telemetry records
replication factor = 3
write quorum = 2
```

## Phase A --- initial cluster

``` text
A B C D E
```

Ingest all records.

Verify:

``` text
10,000 logical records
3 replicas per shard
```

## Phase B --- node removal

Remove:

``` text
C
```

Rebalance.

Verify:

``` text
No logical records lost.
Affected shards have replacement replicas.
```

## Phase C --- node addition

Add:

``` text
F
```

Rebalance.

Verify:

``` text
No logical records lost.
Replication factor restored.
```

## Phase D --- write while one node is unavailable

With one replica unavailable:

``` text
A B X D E
```

Expected:

``` text
2/3 acknowledgements
→ SUCCESS
```

With two replicas unavailable:

``` text
A X X D E
```

Expected for a three-replica shard:

``` text
1/3 acknowledgements
→ FAILURE
```

------------------------------------------------------------------------

# 25. Docker Compose

Only after the local implementation works, run the same storage node as
several containers.

Target:

``` text
node-01
node-02
node-03
node-04
node-05
```

Each container should have:

-   unique node ID
-   unique network address
-   its own local storage
-   access to the same cluster configuration

Conceptually:

``` text
Docker network

+--------+   +--------+   +--------+
| node01 |   | node02 |   | node03 |
+--------+   +--------+   +--------+
     |            |            |
     +------------+------------+
                  |
             +---------+
             | node04  |
             +---------+
                  |
             +---------+
             | node05  |
             +---------+
```

Do not add the later Kafka/Redis, gossip, CRDT, Raft, or LLM services
yet unless they are actually required for the storage milestone.

------------------------------------------------------------------------

# 26. Suggested command workflow

The exact CLI names may differ after implementation, but the final
developer experience should be approximately:

### Start cluster

``` bash
docker compose up --build
```

### Generate telemetry

``` bash
python scripts/generate_telemetry.py --records 10000 --seed 42
```

### Ingest

``` bash
python scripts/start_cluster.py --ingest data/generated/telemetry.jsonl
```

### Verify

``` bash
python scripts/verify_cluster.py
```

### Remove node

``` bash
python scripts/start_cluster.py --remove-node node-03
```

### Rebalance

``` bash
python scripts/start_cluster.py --rebalance
```

### Verify again

``` bash
python scripts/verify_cluster.py
```

The exact command-line interface is an implementation detail. The
behavior is what matters.

------------------------------------------------------------------------

# 27. Unit-test checklist

## Hashing

-   [ ] Same key maps deterministically.
-   [ ] Multiple nodes receive shards.
-   [ ] Three replicas are distinct physical nodes.
-   [ ] Adding a node does not remap every key.
-   [ ] Removing a node does not require rebuilding the entire dataset.

## Replication

-   [ ] Every healthy shard has three replicas.
-   [ ] Replicas are on distinct nodes.
-   [ ] Data exists on each expected replica.
-   [ ] Losing one replica does not lose the logical record.

## Quorum

-   [ ] 2/3 acknowledgements → success.
-   [ ] 3/3 acknowledgements → success.
-   [ ] 1/3 acknowledgements → failure.
-   [ ] 0/3 acknowledgements → failure.

## Rebalancing

-   [ ] Node join works.
-   [ ] Node leave works.
-   [ ] Affected shards are identified.
-   [ ] Replacement replicas are created.
-   [ ] Old copies are removed safely.
-   [ ] No logical records are lost.
-   [ ] Replication factor returns to three.

------------------------------------------------------------------------

# 28. Do not implement these in Week 1

Keep the scope strict.

### Not Week 1

``` text
❌ LLM integration
❌ Agent prompts
❌ Multi-agent reasoning
❌ Gossip protocol
❌ CRDT belief state
❌ Raft leader election
❌ Incident diagnosis
❌ Final dashboard
❌ Production deployment
```

### Week 1 is

``` text
✓ Synthetic telemetry
✓ Sharding
✓ Consistent hashing
✓ Three-way replication
✓ Quorum writes
✓ Node membership
✓ Node join
✓ Node leave
✓ Rebalancing
✓ Verification
✓ Dockerized cluster
```

------------------------------------------------------------------------

# 29. Definition of Done

Week 1 is complete only when all of these are true.

## Data

-   [ ] Synthetic telemetry can be generated reproducibly.
-   [ ] At least 10,000 records can be ingested.
-   [ ] Records are assigned to logical shards.
-   [ ] Shards are distributed across multiple nodes.

## Hashing

-   [ ] Consistent hash ring works.
-   [ ] Virtual nodes are used.
-   [ ] Three distinct physical replica nodes can be selected.
-   [ ] Membership changes update the ring.

## Replication

-   [ ] Replication factor is 3.
-   [ ] Healthy shards have three physical copies.
-   [ ] Data remains readable after one replica disappears.

## Quorum

-   [ ] Write quorum is 2.
-   [ ] 2/3 acknowledgements succeed.
-   [ ] 1/3 acknowledgements fail.
-   [ ] Partial node failure is tested.

## Rebalancing

-   [ ] Node join works.
-   [ ] Node leave works.
-   [ ] Affected shards are identified.
-   [ ] Data moves to replacement replicas.
-   [ ] Replication is restored.
-   [ ] No logical records are lost.

## Testing

-   [ ] Unit tests pass.
-   [ ] End-to-end test passes.
-   [ ] Verification script passes.
-   [ ] Docker Compose runs the cluster.

## Documentation

-   [ ] Architecture is documented.
-   [ ] Setup commands are documented.
-   [ ] Configuration is documented.
-   [ ] Known limitations are documented.

------------------------------------------------------------------------

# 30. Final handoff to Week 2

Once Week 1 is complete, freeze the storage interface.

The next milestone will build:

``` text
                 WEEK 1
           Distributed Store
                  |
                  | get_shard(...)
                  v
                 WEEK 2
              Shard Agent
                  |
                  v
             Hypotheses
```

The agent layer should not need to know:

-   which node owns the shard
-   which replica answered
-   how consistent hashing works
-   how rebalancing works

It should only need an interface equivalent to:

``` python
get_shard(incident_id, time_window)
```

which returns the relevant telemetry.

This separation is important because the later multi-agent experiment
should test **distributed reasoning**, not accidentally couple agent
code to storage internals.

------------------------------------------------------------------------

# 31. Suggested team split

If the group has 4--5 people, split the first milestone by component
while keeping one person responsible for integration.

### Person 1 --- Hash ring

Own:

``` text
hash_ring.py
test_hash_ring.py
```

Responsible for:

-   consistent hashing
-   virtual nodes
-   replica selection
-   membership changes

### Person 2 --- Storage node

Own:

``` text
node.py
models.py
```

Responsible for:

-   telemetry model
-   node API
-   local storage
-   health endpoint

### Person 3 --- Replication + quorum

Own:

``` text
replica_manager.py
quorum.py
test_replication.py
test_quorum.py
```

Responsible for:

-   three-way replication
-   write fan-out
-   acknowledgement tracking
-   quorum decision

### Person 4 --- Rebalancing

Own:

``` text
rebalance.py
test_rebalancing.py
```

Responsible for:

-   node join
-   node leave
-   affected-shard calculation
-   data movement
-   replica restoration

### Person 5 --- Integration / testing

Own:

``` text
store.py
test_end_to_end.py
verify_cluster.py
docker-compose.yml
```

Responsible for:

-   connecting the components
-   end-to-end tests
-   Docker cluster
-   verification output
-   integration debugging

Everyone should review the interfaces before implementation.

------------------------------------------------------------------------

# 32. Interface agreement

Before parallel development begins, agree on these interfaces.

### Storage

``` python
put(record) -> WriteResult
get(shard_key) -> list[TelemetryRecord]
```

### Hash ring

``` python
get_node(shard_key) -> node_id

get_replica_nodes(
    shard_key,
    replication_factor=3
) -> list[node_id]
```

### Node

``` python
store(shard_key, record) -> Ack
get(shard_key) -> list[record]
health() -> HealthStatus
```

### Rebalancer

``` python
rebalance(old_ring, new_ring) -> RebalanceReport
```

Do not change these interfaces casually once the other components depend
on them.

------------------------------------------------------------------------

# 33. What the final Week 1 demo should show

A clean five-minute demo is enough.

### 1. Show the cluster

``` text
node-01
node-02
node-03
node-04
node-05
```

### 2. Ingest telemetry

``` text
10,000 records
```

### 3. Show a shard

Demonstrate:

``` text
Shard X
 ├── node-02
 ├── node-04
 └── node-05
```

### 4. Show quorum

Make one replica unavailable:

``` text
2/3 ACK → SUCCESS
```

Then make two unavailable:

``` text
1/3 ACK → FAILURE
```

### 5. Remove a node

``` text
node-03 leaves
```

Show rebalancing.

### 6. Add a node

``` text
node-06 joins
```

Show that affected shards move.

### 7. Verify

``` text
Logical records before: 10,000
Logical records after:  10,000
Data lost:              0
Replication:            3
```

That is enough to prove the Week 1 deliverable.

------------------------------------------------------------------------

# 34. Final Week 1 result

At the end of this milestone, the team should be able to make this
statement truthfully:

> **"We can partition synthetic production telemetry across a cluster
> using consistent hashing, maintain three replicas for each shard,
> accept writes after a two-replica quorum, and rebalance the dataset
> when nodes join or leave without losing logical records."**

That is the foundation for the actual Fault & Furious experiment.

The later project builds on it:

``` text
WEEK 1
Distributed data
      ↓
WEEK 2
Partial agent views
      ↓
WEEK 3
Gossip + CRDT
      ↓
WEEK 4
Raft + synthesis
      ↓
WEEK 5
Chaos + evaluation
```

**Week 1 deliverable: Working Distributed Telemetry Store.**
