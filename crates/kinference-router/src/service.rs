//! `KinferenceService`: the scoring and pick state behind the gRPC handlers in
//! `crate::grpc`.
//!
//! This module owns the service struct and the value types passed through
//! it. Scoring (the pools, the model index, the scrape reads) is the
//! `Scoring` part in `scoring`; the pick path, as five steps, is in
//! `lifecycle`; tokenizing is [`crate::tokenizer`].

use std::collections::HashMap;
use std::error::Error as StdError;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dynamo_kv_router::indexer::KvIndexerInterface;
use dynamo_kv_router::protocols::{BlockHashOptions, compute_block_hash_for_seq};
use kinference_policy::AUTO_MODEL;

use crate::body_store::{self, Bodies, BodyLimits};
use crate::error::Error;
use crate::metrics::{MetricsConfig, RouterMetrics};
use crate::observations::{Observations, Qos, RequestMeta};
use crate::picker::WorkerScore;
use crate::tokenizer::{self, TokenizeConfig, TokenizeInput};
use crate::warn_once::WarnOnce;
use kinference_fleet::FleetHandles;
use kinference_fleet::models::ModelIndex;
use kinference_fleet::pool::Pool;
use kinference_fleet::scrape::MetricsRegistry;
use kinference_fleet::workers::WorkerEntry;
use kinference_policy::provider_health::ProviderHealthStore;
use kinference_policy::store::PolicyStore;
use prometheus::Registry;
use tokio_util::sync::CancellationToken;

/// The scores from `score_workers`, held in `ScoreOutcome`.
#[derive(Debug, Clone)]
pub struct ScoreResponse {
    pub scores: Vec<WorkerScore>,
}

/// Outcome of [`Scoring::score_workers`].
pub(crate) enum ScoreOutcome {
    Ok(ScoreResponse),
    /// No workers in the registry serve the requested model name.
    NoWorkers,
    /// The prefix-cache signal was lost: `matched_blocks` and `total_blocks`
    /// are 0 on every entry, but the load fields are populated, so the picker
    /// still runs, without the overlap signal.
    Unavailable(ScoreResponse, Degraded),
}

/// Which subsystem cost this pick its prefix-cache signal.
///
/// Both yield a load-only pick, but have different causes: the engine or
/// network, or kinference's own index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Degraded {
    /// Every `/render` attempt across the pool failed, so there are no token
    /// ids to hash and no prefix signal is possible.
    Tokenize,
    /// Tokenization succeeded, but this pool's radix-tree query failed. The
    /// tokens are good; the pool's view of what is cached where is not.
    PrefixIndex,
}

impl Degraded {
    /// Metric label and the operator-facing wording in `PickResponse.reason`.
    pub(crate) fn as_label(self) -> &'static str {
        match self {
            Self::Tokenize => "tokenize",
            Self::PrefixIndex => "prefix_index",
        }
    }

    fn reason(self) -> &'static str {
        match self {
            Self::Tokenize => "tokenize unavailable across the fleet",
            Self::PrefixIndex => "prefix index query failed",
        }
    }
}

/// One `(pool, model)` pair, scored and ready to build candidates from.
///
/// Prefix hashes and block size are per-pool, so the critical section must
/// commit with the scores and side data of the group that actually won.
pub(crate) struct ScoredGroup {
    pub pool: Arc<Pool>,
    /// The upstream model name, which is what the engine serves and what gets
    /// written into the outbound body. Differs from the client-facing name when
    /// an alias resolved.
    pub model: String,
    pub scores: Vec<WorkerScore>,
    pub side: ScoreSideData,
}

/// What the policy chose.
///
/// A provider win has no group (no pool, scores or prefix hashes), hence an
/// enum.
pub(crate) enum PolicySelection {
    /// The policy had no opinion; the ordinary picker decides within `group`.
    /// `None` when there is no local group, e.g. a model only a provider
    /// serves on a pool with no policy.
    Picker { group: Option<ScoredGroup> },
    /// The policy chose this endpoint, inside `group`.
    Local {
        group: ScoredGroup,
        endpoint: String,
    },
    /// The policy chose an off-cluster provider. Nothing pool-scoped applies.
    Provider(ProviderPick),
}

/// A chosen off-cluster destination, flattened for the pick response.
///
/// A backend named by reference, the path to dispatch on, and the vendor's
/// spelling of the model. No URL or credential.
#[derive(Debug, Clone)]
pub(crate) struct ProviderPick {
    pub provider: kinference_policy::engine::ObjectKey,
    pub backend_ref: kinference_policy::engine::ObjectKey,
    pub path: Option<String>,
    /// The vendor's name for the model, written into the body.
    pub upstream_model: String,
    /// The canonical name the request asked for, for logs and status.
    pub canonical_model: String,
}

/// Turn a winning candidate into a [`PolicySelection`].
///
/// The local arm takes the scored group the winner indexes, so the critical
/// section commits against the pool that produced the scores.
fn selection(
    scored: &mut Vec<ScoredGroup>,
    candidate: kinference_policy::engine::Candidate,
) -> PolicySelection {
    match candidate.target {
        kinference_policy::engine::CandidateTarget::Local {
            score_idx,
            endpoint,
            ..
        } => PolicySelection::Local {
            group: scored.swap_remove(score_idx),
            endpoint,
        },
        kinference_policy::engine::CandidateTarget::Provider {
            provider,
            backend_ref,
            path,
            ..
        } => PolicySelection::Provider(ProviderPick {
            provider,
            backend_ref,
            path,
            upstream_model: candidate.upstream_model,
            canonical_model: candidate.canonical_model,
        }),
    }
}

/// Log the winner precisely enough to find it in the ranked candidate list.
fn log_winner(
    pool: &str,
    candidate: &kinference_policy::engine::Candidate,
    trace: &kinference_policy::engine::Trace,
) {
    let (endpoint, objective, path) = match &candidate.target {
        kinference_policy::engine::CandidateTarget::Local {
            endpoint,
            predicted_ttft_ms,
            ..
        } => (
            endpoint.clone(),
            format!("{predicted_ttft_ms:.1}"),
            // A local engine is OpenAI-native, so the path is never rewritten.
            "",
        ),
        kinference_policy::engine::CandidateTarget::Provider {
            backend_ref,
            observed_ttft_ms,
            path,
            ..
        } => (
            backend_ref.label(),
            observed_ttft_ms
                .map(|ms| format!("~{ms:.1}"))
                .unwrap_or_else(|| "unobserved".to_string()),
            path.as_deref().unwrap_or(""),
        ),
    };
    // `path` is logged empty rather than omitted when nothing is rewritten.
    tracing::debug!(
        inference_pool = %pool,
        backend = %candidate.backend,
        class = %candidate.backend_type().as_str(),
        endpoint = %endpoint,
        path = %path,
        predicted_ms = %objective,
        model = %candidate.upstream_model,
        trace = %trace.summary(),
        "policy chose a candidate"
    );
}

/// Candidates for one off-cluster provider and one canonical model name.
///
/// Nothing is scored, queried or tokenized. Every per-endpoint signal is
/// `None` (not a default, since a latency of 0 would win every ranking), and
/// the only latency is the caller-reported p95.
fn provider_candidates_for(
    health: &kinference_policy::provider_health::ProviderHealthStore,
    prov: &kinference_policy::resolve::ResolvedProvider,
    canonical_model: &str,
) -> Vec<kinference_policy::engine::Candidate> {
    let provider = kinference_policy::engine::ObjectKey::new(&prov.namespace, &prov.name);
    // A suppressed provider (repeated dispatch failures) produces no
    // candidates.
    if health.is_suppressed(&provider) {
        return Vec::new();
    }
    let observed_ttft_ms = health.observed_ttft_ms(&provider);
    let labels = Arc::new(prov.labels.clone());
    prov.upstreams_for(canonical_model)
        .into_iter()
        .map(
            |(upstream, context_window)| kinference_policy::engine::Candidate {
                canonical_model: canonical_model.to_string(),
                upstream_model: upstream.clone(),
                backend: prov.name.clone(),
                labels: labels.clone(),
                target: kinference_policy::engine::CandidateTarget::Provider {
                    provider: provider.clone(),
                    backend_ref: prov.backend_ref.clone(),
                    path: prov.path.clone(),
                    // `None` until the first report; still eligible under
                    // EstimatedTTFT.
                    observed_ttft_ms,
                },
                attrs: kinference_policy::predicate::CandidateAttrs {
                    // Provider: no local engine, so neither saturation gauge exists.
                    kv_cache_usage: None,
                    // Declared, not discovered.
                    context_window: context_window.map(|w| w as i64),
                    predicted_ttft_ms: None,
                    prefix_cache_hit_ratio: None,
                    queued_prompts: None,
                    model_names: policy_driver::model_names(
                        &upstream,
                        prov.aliases_for(&upstream),
                        canonical_model,
                    ),
                    backend_name: Some(prov.name.clone()),
                    backend_type: Some(
                        kinference_policy::crd::BackendType::Provider
                            .as_str()
                            .to_string(),
                    ),
                    backend_ready_endpoints: None,
                    backend_observed_ttft_ms: observed_ttft_ms.map(|ms| ms as f64),
                    // No source yet; compile warns when a policy names it.
                    backend_budget_exhausted: None,
                },
            },
        )
        .collect()
}

/// Byproducts of `score_workers` the pick path needs after the pick.
///
/// `WorkerScore.matched_blocks` carries the raw radix-tree match; α-decay
/// layering happens later, inside the `pick_lock` critical section.
pub(crate) struct ScoreSideData {
    /// Block hashes, one list per block size. The pick path looks up the
    /// chosen worker's to record the pending prefix.
    pub hashes_by_block_size: HashMap<u32, Vec<u64>>,
    /// The resolved token IDs.
    ///
    /// Tokenization depends on the model, not the pool, so a policy scoring
    /// one model across several pools tokenizes once and re-scores via
    /// [`Input::Tokens`].
    pub tokens: Vec<u32>,
}

pub struct KinferenceService {
    pub(crate) scoring: Scoring,
    /// The tokenizer. The mode and render URL are per pool, on the request.
    pub(crate) tokenizer: Arc<dyn tokenizer::Tokenizer>,
    /// Buffered request bodies for the `PushBody` → `Pick` → `PullBody` flow.
    pub(crate) bodies: Bodies,
    /// What each pick leaves behind until the caller reports.
    pub(crate) observations: Observations,
    /// CRD-sourced routing policy, when `policy.enabled = true`. When `None`,
    /// a request naming an `InferencePool` falls back to model-based routing.
    pub(crate) policy: Option<Arc<PolicyStore>>,
    /// The metrics kinference publishes on its own `/metrics`.
    pub(crate) prom: Arc<RouterMetrics>,
    /// Unknown `InferencePool`s that requests have named, so each logs once.
    unknown_pools: WarnOnce<String>,
}

/// What the control plane publishes and the router reads.
pub struct PolicyHandles {
    /// `None` when policy is off: a request naming a pool falls back to
    /// model-based routing.
    pub store: Option<Arc<PolicyStore>>,
    /// Shared with the status writer, which publishes what the feedback
    /// RPCs record here.
    pub provider_health: Arc<ProviderHealthStore>,
}

/// The router's settings, resolved by the server from its config.
pub struct RouterConfig {
    pub tokenize: TokenizeConfig,
    pub metrics: MetricsConfig,
    pub bodies: BodyLimits,
    /// How long a pick's bookkeeping waits for the caller's report. Must
    /// exceed the worst-case full response time, since a non-streaming
    /// caller only reports at completion.
    pub observation_ttl: Duration,
}

/// How often the body and observation sweeps run.
const SWEEP_INTERVAL: Duration = Duration::from_secs(10);

impl KinferenceService {
    pub(crate) fn new(
        scoring: Scoring,
        tokenizer: Arc<dyn tokenizer::Tokenizer>,
        bodies: Bodies,
        observations: Observations,
        policy: Option<Arc<PolicyStore>>,
        prom: Arc<RouterMetrics>,
    ) -> Self {
        Self {
            scoring,
            tokenizer,
            bodies,
            observations,
            policy,
            prom,
            unknown_pools: WarnOnce::default(),
        }
    }

    /// Build the service and start what runs beside it: the tokenizer
    /// warm-up and the body and observation sweeps, all until `cancel`.
    /// Registers the router's metric families in `registry`. Call it before
    /// the fleet starts, so the warm-up sees every model claim.
    pub fn start(
        config: RouterConfig,
        fleet: FleetHandles,
        policy: PolicyHandles,
        registry: &Registry,
        cancel: CancellationToken,
    ) -> Result<Arc<Self>, Error> {
        let prom = Arc::new(RouterMetrics::new(registry, &config.metrics));
        let limits = config.bodies;
        let store =
            body_store::shared(limits.max_total_bytes, limits.max_request_bytes, limits.ttl);
        let tokenizer = tokenizer::build(
            &config.tokenize,
            fleet.scrape.clone(),
            prom.clone(),
            fleet.pools.clone(),
            &fleet.models,
            cancel.clone(),
        )?;
        let service = Arc::new(Self::new(
            Scoring::new(fleet.pools, fleet.models, fleet.scrape),
            tokenizer,
            Bodies::new(store, limits.idle_timeout, prom.clone()),
            Observations::new(config.observation_ttl, policy.provider_health, prom.clone()),
            policy.store,
            prom,
        ));
        service.clone().spawn_body_sweep(cancel.clone());
        service.clone().spawn_observation_sweep(cancel);
        Ok(service)
    }

    /// Reclaim buffered bodies past their TTL and refresh the occupancy
    /// gauges. Separate from the observation sweep since the TTLs differ
    /// greatly; it guarantees release, `Abort` only shortens the window.
    fn spawn_body_sweep(self: Arc<Self>, cancel: CancellationToken) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(SWEEP_INTERVAL);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = interval.tick() => {
                        self.bodies.sweep();
                    }
                }
            }
        });
    }

    /// Evict pending observations past their TTL, which releases each
    /// expired pick's inflight state (it has no sweep of its own) and bounds
    /// the stash, and drop providers with no live samples or failures.
    fn spawn_observation_sweep(self: Arc<Self>, cancel: CancellationToken) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(SWEEP_INTERVAL);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = interval.tick() => {
                        let evicted = self.observations.sweep_expired();
                        if evicted > 0 {
                            tracing::debug!(
                                evicted,
                                "evicted expired observations"
                            );
                        }
                        let dropped = self.observations.provider_health.sweep();
                        if dropped > 0 {
                            tracing::debug!(
                                dropped,
                                "dropped idle provider health entries"
                            );
                        }
                    }
                }
            }
        });
    }

    /// Mint a fresh `response_id` for a new pick.
    ///
    /// Random, so a peer can't guess another caller's id and `PullBody` its
    /// body, and never 0, which means "no id" on the wire. Collisions are
    /// negligible at 64 bits.
    pub(crate) fn mint_response_id(&self) -> u64 {
        loop {
            let mut buf = [0u8; 8];
            let id = match getrandom::getrandom(&mut buf) {
                Ok(()) => u64::from_le_bytes(buf),
                Err(e) => {
                    tracing::error!(
                        error = &e as &dyn StdError,
                        "getrandom failed while minting a response_id"
                    );
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_nanos() as u64)
                        .unwrap_or(1)
                }
            };
            if id != 0 {
                return id;
            }
        }
    }

    /// Every client-facing name declared for `upstream` across the named pool's
    /// worker sets, sorted, deduped, and excluding `upstream` itself.
    ///
    /// Feeds `RequestAttrs.model` so a predicate can match the client's
    /// spelling. Empty when no pool is named or policy is off.
    fn declared_aliases(
        &self,
        pool_ref: Option<&kinference_policy::PoolRef>,
        upstream: &str,
    ) -> Vec<String> {
        let Some(store) = self.policy.as_ref() else {
            return Vec::new();
        };
        let Some(pref) = pool_ref else {
            return Vec::new();
        };
        let snapshot = store.snapshot();
        let Some(resolved) = snapshot.lookup(&pref.namespace, &pref.name) else {
            return Vec::new();
        };
        // Local alias tables only; a provider's rows map the other way.
        let mut out: Vec<String> = resolved
            .local_destinations()
            .flat_map(|d| d.aliases_for(upstream))
            .filter(|a| a != upstream)
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// Translate a client-facing alias into an upstream model an engine
    /// actually serves, using the alias tables on the named pool's worker sets.
    ///
    /// `None` when there is nothing to translate: no pool named, policy off,
    /// the pool unresolved, or no declared alias whose upstream is served.
    ///
    /// Only reached after `resolve_pools` came back empty, so this never
    /// shadows a model an engine reports under its own name. Local
    /// destinations only; providers map names when candidates are built.
    fn resolve_alias(
        &self,
        pool_ref: Option<&kinference_policy::PoolRef>,
        canonical: &str,
    ) -> Option<String> {
        let store = self.policy.as_ref()?;
        let pref = pool_ref?;
        let snapshot = store.snapshot();
        let resolved = snapshot.lookup(&pref.namespace, &pref.name)?;
        let local: Vec<&kinference_policy::resolve::ResolvedWorkerSet> =
            resolved.local_destinations().collect();
        first_served_upstream(&local, canonical, |m| {
            !self.scoring.resolve_pools(m).is_empty()
        })
    }

    /// Whether any provider in the named pool declares this canonical model.
    ///
    /// Consulted after `resolve_pools` came back empty, so a model only a
    /// provider serves isn't refused before `policy_select` runs.
    fn provider_serves(&self, pool_ref: Option<&kinference_policy::PoolRef>, model: &str) -> bool {
        let Some(store) = self.policy.as_ref() else {
            return false;
        };
        let Some(pref) = pool_ref else {
            return false;
        };
        let snapshot = store.snapshot();
        let Some(resolved) = snapshot.lookup(&pref.namespace, &pref.name) else {
            return false;
        };
        resolved
            .destinations
            .iter()
            .filter_map(|d| d.as_provider())
            .any(|p| !p.upstreams_for(model).is_empty())
    }
}

/// Project a scraped sample onto a [`WorkerScore`], for both `score_workers`
/// and [`Scoring::degraded_response`].
///
/// The prefix-cache values are parameters because the degraded path zeroes
/// them while still reporting real load.
fn worker_score(
    entry: &WorkerEntry,
    sample: &kinference_fleet::scrape::MetricsSample,
    block_size: u32,
    matched_blocks: u32,
    total_blocks: u32,
) -> WorkerScore {
    WorkerScore {
        name: entry.worker_id_str.clone(),
        endpoint: entry.score_addr.clone(),
        matched_blocks,
        completion_rate_per_sec: sample.completion_rate_per_sec,
        queue_waiting: sample.waiting,
        queue_running: sample.running,
        queue_swapped: sample.swapped,
        kv_cache_usage: sample.kv_cache_usage,
        block_size,
        total_blocks,
    }
}

/// Input variants `score_workers` accepts. The Pick handler uses `Messages`;
/// policy re-scoring uses `Tokens`.
#[derive(Clone)]
pub(crate) enum Input {
    #[allow(dead_code)]
    Raw(String),
    #[allow(dead_code)]
    Chat(String),
    /// OpenAI-style messages array, already parsed.
    Messages(Vec<serde_json::Value>),
    #[allow(dead_code)]
    Tokens(Vec<u32>),
}

/// What one `(pool, model)` is scored from.
#[derive(Clone)]
pub(crate) struct ScoreInput {
    pub input: Input,
    pub chat_extras: ChatExtras,
    /// The body `/render` sees. `None` with `Input::Tokens`.
    pub raw_body: Option<Vec<u8>>,
}

impl ScoreInput {
    /// Tokens already resolved for the model, so `/render` is not called
    /// again.
    pub(crate) fn tokens(tokens: Vec<u32>) -> Self {
        Self {
            input: Input::Tokens(tokens),
            chat_extras: ChatExtras::default(),
            raw_body: None,
        }
    }
}

/// First upstream among `destinations`' alias tables that `is_served` accepts.
///
/// Split out of `KinferenceService::resolve_alias` for testing. Skips the
/// canonical name itself, which no pool serves if we got here. Takes the first
/// match, so interchangeable upstreams resolve to one fixed choice.
pub fn first_served_upstream(
    destinations: &[&kinference_policy::resolve::ResolvedWorkerSet],
    canonical: &str,
    is_served: impl Fn(&str) -> bool,
) -> Option<String> {
    destinations
        .iter()
        .flat_map(|d| d.upstreams_for(canonical))
        .find(|u| u != canonical && is_served(u))
}

/// Pick the pool a no-policy request should be served from.
///
/// Each group is scored by its best endpoint under
/// [`crate::picker::predicted_wait_ms`], the same term the policy path's
/// `candidates_for` uses.
///
/// Only the pool is decided here; the endpoint is chosen afterwards under
/// that pool's `pick_lock`. Ranking without locks avoids deadlocks between
/// requests taking two pools' locks in opposite orders.
///
/// Groups whose scores are empty cannot win; `None` means no pool serving
/// the model had a usable worker.
fn best_group(
    groups: Vec<ScoredGroup>,
    // `Option` so tests can omit it. Predictions are recorded here, where
    // they are computed per endpoint.
    prom: Option<&crate::metrics::RouterMetrics>,
) -> Option<ScoredGroup> {
    let now = Instant::now();
    let multi = groups.len() > 1;
    let mut best: Option<(f32, ScoredGroup)> = None;
    for group in groups {
        let Some(wait) = group
            .scores
            .iter()
            .map(|s| {
                let summaries = group.pool.inflight_prefixes.summaries_for(&s.endpoint);
                let ms =
                    crate::picker::predicted_wait_ms(s, &group.pool.predictor, &summaries, now)
                        .total_ms();
                // Every endpoint's prediction is recorded, as on the policy
                // path.
                if let Some(p) = prom {
                    p.record_predicted_ttft(&s.endpoint, &group.model, ms as f64);
                }
                ms
            })
            .fold(None::<f32>, |acc, ms| Some(acc.map_or(ms, |a| a.min(ms))))
        else {
            continue;
        };
        if multi {
            tracing::debug!(
                workerset = %group.pool.name,
                endpoints = group.scores.len(),
                best_predicted_wait_ms = wait,
                "scored a worker set"
            );
        }
        // Strictly-better keeps the first pool on a tie, and `serving` is in
        // pool-id order, so ties resolve the same way on every replica.
        if best.as_ref().is_none_or(|(b, _)| wait < *b) {
            best = Some((wait, group));
        }
    }
    let (wait, group) = best?;
    if multi {
        tracing::debug!(
            workerset = %group.pool.name,
            predicted_wait_ms = wait,
            "chose a worker set"
        );
    }
    Some(group)
}

/// OpenAI ChatCompletionRequest fields beyond `messages` that local
/// chat-template rendering needs to match the engine's tokens. Ends up in
/// [`crate::tokenizer::TokenizeInput::Chat`].
///
/// The default has no tools and `add_generation_prompt = true`, the OpenAI
/// default.
#[derive(Debug, Clone)]
pub struct ChatExtras {
    pub tools: Vec<serde_json::Value>,
    pub tool_choice: Option<serde_json::Value>,
    pub add_generation_prompt: bool,
}

impl Default for ChatExtras {
    fn default() -> Self {
        Self {
            tools: Vec::new(),
            tool_choice: None,
            add_generation_prompt: true,
        }
    }
}

/// Resolved input: pre-supplied tokens, or input for
/// the tokenizer.
pub(crate) enum TokenSource {
    Direct(Vec<u32>),
    NeedsTokenize(TokenizeInput),
}

/// Outcome of [`KinferenceService::pick_and_record`]. For `Ok` and `Degraded`
/// the side effects (inflight increment, inflight-prefix insert,
/// pending-observation stash) have been applied; the error outcomes write no
/// state.
#[derive(Debug)]
pub(crate) enum PickAndRecordOutcome {
    /// Successful pick. The caller dispatches to `endpoint` and later reports
    /// the observed TTFT.
    Ok {
        endpoint: String,
        response_id: u64,
        /// Rewritten request body, or empty when unchanged, in which case
        /// `grpc::pick` reuses the pushed body. See [`crate::pick_body`].
        body: Vec<u8>,
        /// The model this request resolved to (post-`auto`). Surfaced as
        /// `PickResponse.requested_model`.
        model: String,
    },
    /// The prefix-cache signal was unavailable, so the pick was made on load
    /// alone. Otherwise like `Ok`.
    Degraded {
        endpoint: String,
        response_id: u64,
        reason: String,
        /// Rewritten request body; empty when unchanged (see `Ok::body`).
        body: Vec<u8>,
        /// The model this request resolved to (post-`auto`).
        model: String,
    },
    /// A policy chose an off-cluster provider. The body names the vendor's
    /// model and a pending observation is held, but nothing pool-scoped was
    /// touched. `outcome=OK` on the wire, with a `ProviderTarget` instead of
    /// an endpoint.
    Provider {
        pick: ProviderPick,
        response_id: u64,
        /// Rewritten request body. Never empty, since the vendor's model name
        /// always differs.
        body: Vec<u8>,
    },
    /// No ready worker serves the requested model. `model`, the name the
    /// picker tried, appears in the response message. `outcome=NO_WORKERS`.
    NoWorkers { model: String },
    /// A policy refused the request, though workers may be available.
    /// `outcome=POLICY_REJECTED`.
    PolicyRejected { reason: String, model: String },
    /// The body isn't a valid OpenAI ChatCompletionRequest, or has no model.
    /// `model` is the best-known name, often empty.
    BadRequest { reason: String, model: String },
}

mod lifecycle;
mod policy_driver;
mod scoring;

pub(crate) use scoring::Scoring;

#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;
