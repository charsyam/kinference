# Architecture

Kinference picks the endpoint that serves an LLM request fastest. A gateway
streams the request body to Kinference and asks for a pick. Kinference
tokenizes the prompt, matches the prompt's prefix against the blocks that
each worker has cached, reads each worker's load from the last metrics
scrape, predicts a wait time for each worker, and returns the winner. If
the request named an `InferencePool`, the pool's `InferencePolicy` first
narrows and ranks the candidates, and the winner may be a provider outside
the cluster. Later the caller reports the first and the final token. That
report closes the pick's in-flight state and leaves a row that operators
use to tune the predictor. A control plane watches the custom resources,
turns worker sets into pools and writes their status. The replicas elect
one of themselves to do the writing.

This page is the map: where code lives, which way imports go, what must
stay true, and where new code goes. Change it in the same pull request as
any move. To follow one request step by step, read
[docs/request-flow.md](docs/request-flow.md).

## Vocabulary

- Worker: one vLLM engine process. It serves `/metrics` and `/v1/models`
  over HTTP and publishes KV-cache events over ZMQ.
- Worker set: a group of workers with one block size and one profile. An
  `InferenceWorkerSet` or the config declares a worker set, and both
  resolve to a `PoolSpec`.
- Pool: the runtime state of exactly one worker set: its radix tree, its
  worker registry, its in-flight bookkeeping and the lock that serializes
  commits. Kinference never merges pools, but one pick may score several.
- Profile: the tuning that a pool uses: predictor coefficients, picker
  policy and the tokenize mode. The config names each profile, and worker
  sets reference a profile by that name.
- Block: a fixed run of tokens, as long as the worker set's `kvBlockSize`.
  Engines publish cache events per block, so the block is the unit of
  prefix matching.
- Index: the radix tree of blocks that each worker has cached. Kinference
  builds it from the KV events, one index per pool.
- Pick: one routing decision. The body goes in; an endpoint and a request
  id come out.
- In-flight: picks that Kinference has dispatched but not yet observed,
  and dispatched prefixes that are not yet in the index. Both credit a
  worker before the engine reports.
- Observation: what a pick leaves behind until the caller reports.
  Finishing an observation releases the in-flight state and logs an event
  of predicted versus observed latency.
- `InferencePool`: the resource that names the destinations that a route
  may reach, and the policy that chooses among them. Not a pool.
- Provider: a destination outside the cluster, which an
  `InferenceProvider` declares. The caller dials it; Kinference learns the
  provider's health only from what the caller reports.
- Snapshot: the resolved view of pools, worker sets, providers and
  compiled policies. The controller publishes it; the router and the admin
  API read it.
- `BackendBinding`: the object that the controller writes for each
  `InferencePool`, so that the gateway can resolve a backend reference that
  it has no schema for.
- Leader: the one replica that writes status and bindings. Every replica
  routes.

## Entry points

- `server::run`, in `src/server.rs`. The binary. It loads the config,
  makes the one metrics registry, builds the fleet, starts the router,
  starts the fleet, starts the control plane, spawns the admin listener and
  serves gRPC until a signal arrives. Then it cancels and waits for the
  control plane.
- `Fleet::new` and `Fleet::start`, `KinferenceService::start` and
  `controller::start`. The three subsystems. The server builds each one
  once, from its settings and a struct of shared handles.
- `router::grpc`, the `Picker` handlers. `lifecycle::pick_and_record` is
  the pick itself, in five typed steps.
- `crates/kinfctl/src/main.rs`, the CLI. It reads manifests and runs the
  policy crate's checks without a cluster.
- `crates/kinference-router/proto/kinference.proto`, the wire contract.
  Its header documents the steps of each request and the error model.

## Code map

Each concern has one crate. Each crate's `lib.rs` lists its modules and
what they own; this map stays at the crate level.

### `kinference-policy`, in `crates/kinference-policy`

The ids, the CRD types, and every part of routing policy that the code can
compute without a cluster: the predicate grammar, the compile step that
runs once per `InferencePolicy` revision, the resolution of the resources
into the snapshot, candidate generation and selection, the explain trace,
the lint and the manifest checks behind `kinfctl check`. The crate also
holds the snapshot store that the controller fills and the router reads.

**Invariant.** No client, no runtime, no I/O. `resolve` is a pure function
over the CRD contents, so the router's watcher and `kinfctl` reach the same
answer from the same inputs.

### `kinference-fleet`, in `crates/kinference-fleet`

Which workers exist and what state they are in. Pools and the pool
manager, the worker registry, the model index, the coordinator that takes
discovery and eviction events, the scrape of engine metrics, the ZMQ ingest
of KV events, the in-flight trackers and the fleet's gauges.

**Invariants.** The fleet never holds a Kubernetes client: the server
supplies discovery as a task, through a factory. Fleet-wide state uses the
worker id as its key, and the first pool that registers a worker owns it.
Pools are independent and share no radix trees.

### `kinference-router`, in `crates/kinference-router`

The request path. The gRPC glue, the body store that holds bodies between
push and pull, the tokenizer, scoring, the policy driver, the picker, the
pick lifecycle and the observations that a pick leaves behind.

**Invariants.** The picker is pure: it takes inputs that the router has
already fetched and returns a ranking. `grpc` maps proto types to service
calls and holds no logic. Only `commit` writes state, and only under the
pool's pick lock. Policy ranking holds no pool lock.

### `kinference-controller`, in `crates/kinference-controller`

The Kubernetes control plane. The CRD watch that feeds the snapshot, the
worker-set reconcile that turns `InferenceWorkerSet`s into pools, the
status and binding writers, leader election over a Lease, and the
EndpointSlice discovery that the fleet runs.

**Invariants.** It is the one crate that depends on `kube`. Everything that
talks to the API server goes through `api::ApiServer`, which has a live
implementation and a recorded one for tests. Only the leader writes.

### `kinference`, the server crate at the top of `src`

The operator surface: the config and its validation, the wiring in
`server`, the admin listener with `/readyz`, `/metrics` and the read-only
policy API, the Prometheus exposition and the version string.

**Invariant.** It is the only crate that reads the environment, and it
reads the environment only at startup. It is also the only crate that uses
`anyhow`.

### `kinfctl`, in `crates/kinfctl`

The CLI. It imports `kinference-policy` and nothing else of ours.

### Import direction

- `kinference-policy` imports nothing of ours.
- The fleet imports `kinference-policy`.
- The router imports the fleet and `kinference-policy`.
- The controller imports the fleet and `kinference-policy`, never the
  router.
- The server crate imports all of the above.

`kube`, `reqwest`, `tonic`, `axum` and `zmq` stay at the edges. Decision
logic takes data and returns data.

## Cross-cutting concerns

### Configuration

The server crate's `config` module loads one `Config` at startup and
validates it. Each subsystem declares its settings as a struct next to the
code that reads them; `config` assembles and checks them. Nothing reads an
environment variable after startup.

### Errors

Each library crate has its own hand-written `Error` enum. Only the binary
uses `anyhow`, to add context before it exits.

### Metrics

The server creates the one Prometheus registry and lends it to each crate.
Each crate has a metrics struct that registers the crate's families through
the helpers in `fleet::metrics`, and the crate records each metric where
the event happens. Metric names start with `kinference_`. The admin
listener renders the registry; it owns no family of its own beyond
`build_info`.

### Locking

Kinference uses standard mutexes and takes them through the helpers in
`fleet::sync` and `controller::sync`, which recover from poisoning. Every
guarded map changes only by single inserts and removes or by a swap of the
whole value, so a panic elsewhere cannot leave a map torn. Stale but whole
data is better than a panic that spreads. The pool's pick lock is the one
critical section on the request path.

### Shutdown and leadership

`server` creates one `CancellationToken` and clones it into every task.
SIGTERM or SIGINT stops the gRPC server. Then `server` cancels the token
and gives the control plane a bounded moment to vacate its Lease, so that
the peer takes over at once. Leadership gates only the status and binding
writers; routing never waits on it.

### Observability

Kinference writes structured logs through `tracing`, and `RUST_LOG`
filters them. `KINFERENCE_LOG_FORMAT=json` switches the logs from text to
JSON. Metrics work as described above. For each streaming pick, the
router logs a debug event under the `kinference::observation` target, with
the predicted and the observed times. Operators use these events to refit
the predictor.

### Testing

Unit tests sit next to their module, in a sibling `tests.rs`. The
controller tests run against `api::Recorded`, which answers from objects
that the test supplies, so no test needs a cluster. The router's tokenizer
and service have `test_support` modules for the same reason.
`tests/fake_vllm.py` publishes KV events for local runs, and
`tests/e2e/smoke.sh` runs the real engine with dummy weights on a kind
cluster.

`tests/bench_inflight.py` runs the fleet's ignored tracker benchmarks in
isolated processes and samples RSS. The runner pools completed picks to
report throughput and latency percentiles.

## Where new code goes

- A new signal for scoring. Collect it in the fleet, in `scrape` or
  `ingest`. Store it in the registry. Read it in the router's scoring. The
  picker stays pure.
- A new RPC or proto field. Add it to the proto; then `router::grpc` maps
  it to a service call. No logic in `grpc`.
- A new metric. Add it to the metrics struct of the crate where the event
  happens, through the helpers in `fleet::metrics`. Record it there.
- A new config setting. Add it to the config struct of the subsystem that
  reads it, next to that code. Validate it in `config`. Give it a type, not
  a raw string.
- A new CRD field or policy rule. Put the types and the pure logic in
  `kinference-policy`. The controller writes the status. The router reads
  the snapshot. `kinfctl` gets it for free.
- A new engine kind. Put the metric names in `scrape` and the event
  parsing in `ingest`.
- A new admin endpoint. Put it in `admin`. It reads snapshots and never
  writes router state.
- Anything that talks to the API server. Put it in the controller, and go
  through `api::ApiServer`, never a `kube::Api` of its own.

## Rules for a move

- Behaviour stays the same. Tests, clippy and fmt stay green after every
  commit.
- One structural change per commit. Move, test, commit. Rename, test,
  commit.
- No constructor takes more than six arguments. Group the handles in a
  struct that `server` builds once.
- Add a trait only where two implementations exist.
- A module's `//!` header says what the module owns and the one rule that
  a caller must respect. The crate's `lib.rs` lists its modules, one line
  each.
