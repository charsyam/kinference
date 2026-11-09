//! The CRD policy driver: running `InferencePolicy` for a request that named
//! an `InferencePool`.
//!
//! `policy_select` applies a compiled policy to a live snapshot of pools,
//! worker sets and providers; `candidates_for` flattens a scored group into
//! per-endpoint candidates. Ranking holds no pool lock; see `policy_select`.

use super::*;

impl KinferenceService {
    /// Run the CRD policy engine for a request that named an `InferencePool`.
    ///
    /// Candidates may span pools, each with its own `pick_lock`, so ranking is
    /// lock-free (taking two locks in opposite orders would deadlock) and only
    /// the winner's pool is locked, to commit. The ranking may be one
    /// concurrent pick stale; inflight bookkeeping is still exact.
    ///
    /// `PolicySelection::Picker` means the policy had no opinion, e.g. for a
    /// pool kinference doesn't know, and the normal picker chooses (fail
    /// open).
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn policy_select(
        &self,
        pref: &kinference_policy::PoolRef,
        store: &Arc<kinference_policy::store::PolicyStore>,
        primary: Option<ScoredGroup>,
        // Passed explicitly, since `primary` may not exist (a provider-only
        // model).
        requested_model: &str,
        spare: Option<ScoreInput>,
        attrs: kinference_policy::predicate::RequestAttrs,
    ) -> Result<PolicySelection, PickAndRecordOutcome> {
        use kinference_policy::engine;

        let snapshot = store.snapshot();
        let Some(resolved) = snapshot.lookup(&pref.namespace, &pref.name) else {
            // Both fail open, but are logged differently: before the first list
            // completes, every pool looks unknown.
            if snapshot.listed() {
                let pool = pref.label();
                if self.unknown_pools.first(pool.as_str()) {
                    tracing::warn!(
                        inference_pool = %pool,
                        "unknown InferencePool; routing on load alone"
                    );
                }
                self.prom.record_policy_decision("unknown_pool");
            } else {
                tracing::debug!(
                    inference_pool = %pref.label(),
                    "InferencePools not listed yet; routing on load alone"
                );
                self.prom.record_policy_decision("not_listed");
            }
            return Ok(PolicySelection::Picker { group: primary });
        };

        // Opaque revision: we can tell we differ, not who is behind. Advisory,
        // so record it and proceed.
        if !pref.resource_version.is_empty() && pref.resource_version != resolved.resource_version {
            self.prom.record_policy_revision_mismatch();
            tracing::debug!(
                inference_pool = %pref.label(),
                requested = %pref.resource_version,
                have = %resolved.resource_version,
                "InferencePool revision differs from the gateway's"
            );
        }

        let model = requested_model.to_string();
        // Rules can't reach beyond the pool's destinations.
        if resolved.destinations.is_empty() {
            self.prom.record_policy_decision("no_destinations");
            return Err(PickAndRecordOutcome::PolicyRejected {
                reason: format!(
                    "pool {} lists no destination kinference can route to",
                    pref.label()
                ),
                model,
            });
        }

        let policy = resolved.policy.clone();
        let rules = engine::matching_rules(&policy, &attrs);
        let any_rule_matched = !rules.is_empty();
        // Matching rules, then the implicit unmatched route (`None`, traced as
        // `<unmatched>`). The unmatched route gets its own candidate generation
        // (the requested model, local destinations only), so it can't serve a
        // model an earlier rule scored.
        let mut plan: Vec<(Option<String>, kinference_policy::compile::CompiledRoute)> = rules
            .iter()
            .map(|r| (Some(r.name.clone()), r.route.clone()))
            .collect();
        match engine::unmatched_route(&policy) {
            Ok(route) => plan.push((None, route)),
            Err(_) if !any_rule_matched => {
                // `unmatched: Reject`.
                tracing::debug!(
                    inference_pool = %pref.label(),
                    "no policy rule matched; refusing"
                );
                self.prom.record_policy_decision("rejected_unmatched");
                return Err(PickAndRecordOutcome::PolicyRejected {
                    reason: "no rule matched and unmatched=Reject".to_string(),
                    model,
                });
            }
            // Rules matched. If every one falls through, the refusal comes
            // after the loop.
            Err(_) => {}
        }

        // Groups scored so far, by (pool, model). Others are scored lazily
        // below.
        let mut scored: Vec<ScoredGroup> = primary.into_iter().collect();

        for (rule_name, route) in &plan {
            // The name that the client asked for. The loop below resolves it
            // through each destination's own alias table; see
            // `engine::models_to_score`.
            let targets = engine::models_to_score(route, &attrs.requested_model);
            let mut candidates: Vec<engine::Candidate> = Vec::new();
            // For each (destination, upstream), the indices of the candidates
            // that the loop has built so far. Two targets can lead to the same
            // pair, as when `consider` contains both a model and its alias. A
            // copy per name would record its prediction once per copy and tilt
            // `tieBreak: Random` toward that destination.
            let mut built: HashMap<(CandidateDest, String), Vec<usize>> = HashMap::new();

            for target in &targets {
                for dest in &resolved.destinations {
                    if !target
                        .backends
                        .matches(dest.backend_type(), dest.name(), dest.labels())
                    {
                        continue;
                    }
                    // Providers need no scoring; candidates come from the
                    // declared model table.
                    let dest = match dest {
                        kinference_policy::resolve::ResolvedDestination::Local(local) => local,
                        kinference_policy::resolve::ResolvedDestination::Provider(prov) => {
                            let dest =
                                CandidateDest::Provider(prov.namespace.clone(), prov.name.clone());
                            for c in provider_candidates_for(
                                &self.observations.provider_health,
                                prov,
                                &target.model,
                            ) {
                                let key = (dest.clone(), c.upstream_model.clone());
                                if !add_name(&mut candidates, &built, &key, &target.model) {
                                    built.insert(key, vec![candidates.len()]);
                                    candidates.push(c);
                                }
                            }
                            continue;
                        }
                    };
                    // A canonical name can map to several upstream models, each
                    // its own candidate.
                    for upstream in dest.upstreams_for(&target.model) {
                        let key = (CandidateDest::Local(dest.pool_id), upstream.clone());
                        if add_name(&mut candidates, &built, &key, &target.model) {
                            continue;
                        }
                        let idx = match scored
                            .iter()
                            .position(|g| g.pool.id == dest.pool_id && g.model == upstream)
                        {
                            Some(i) => i,
                            None => {
                                let Some(pool) =
                                    self.scoring.pools.snapshot().get(dest.pool_id).cloned()
                                else {
                                    continue;
                                };
                                let Some(spare) = spare.as_ref() else {
                                    // Only reachable if scoring inputs were not
                                    // cloned, which cannot happen when a policy
                                    // is active.
                                    continue;
                                };
                                // Reuse tokens if this model was already
                                // tokenized for another pool.
                                let cached = scored
                                    .iter()
                                    .find(|g| g.model == upstream && !g.side.tokens.is_empty())
                                    .map(|g| g.side.tokens.clone());
                                let input = match cached {
                                    Some(t) => ScoreInput::tokens(t),
                                    None => spare.clone(),
                                };
                                let (outcome, side) = self
                                    .scoring
                                    .score_workers(
                                        &*self.tokenizer,
                                        &pool,
                                        &upstream,
                                        input,
                                        HashMap::new(),
                                    )
                                    .await;
                                let s = match outcome {
                                    ScoreOutcome::Ok(r) => r.scores,
                                    ScoreOutcome::Unavailable(r, why) => {
                                        self.prom.record_score_degraded(why.as_label());
                                        r.scores
                                    }
                                    // This worker set doesn't serve the model.
                                    ScoreOutcome::NoWorkers => continue,
                                };
                                if s.is_empty() {
                                    continue;
                                }
                                scored.push(ScoredGroup {
                                    pool,
                                    model: upstream.clone(),
                                    scores: s,
                                    side,
                                });
                                scored.len() - 1
                            }
                        };
                        let new = self.candidates_for(&scored[idx], idx, dest, &target.model);
                        built.insert(
                            key,
                            (candidates.len()..candidates.len() + new.len()).collect(),
                        );
                        candidates.extend(new);
                    }
                }
            }

            // Log the cross-group ranking, which the per-group score table
            // can't show. One line per candidate, best first, capped; `rank` and
            // `n` show when the list is truncated.
            if tracing::enabled!(tracing::Level::TRACE) {
                // Providers have no prediction, so they sort last.
                let sort_key = |c: &engine::Candidate| match &c.target {
                    engine::CandidateTarget::Local {
                        predicted_ttft_ms, ..
                    } => *predicted_ttft_ms,
                    engine::CandidateTarget::Provider { .. } => f32::INFINITY,
                };
                let mut ranked: Vec<&engine::Candidate> = candidates.iter().collect();
                ranked.sort_by(|a, b| {
                    sort_key(a)
                        .partial_cmp(&sort_key(b))
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                const SHOWN: usize = 8;
                let total = ranked.len();
                for (i, c) in ranked.iter().take(SHOWN).enumerate() {
                    let (endpoint, predicted) = match &c.target {
                        engine::CandidateTarget::Local {
                            endpoint,
                            predicted_ttft_ms,
                            ..
                        } => (endpoint.clone(), format!("{predicted_ttft_ms:.1}")),
                        engine::CandidateTarget::Provider {
                            backend_ref,
                            observed_ttft_ms,
                            ..
                        } => (
                            backend_ref.label(),
                            observed_ttft_ms
                                .map(|ms| format!("~{ms:.1}"))
                                .unwrap_or_else(|| "unobserved".to_string()),
                        ),
                    };
                    tracing::trace!(
                        rule = rule_name.as_deref().unwrap_or("<unmatched>"),
                        rank = i + 1,
                        n = total,
                        backend = %c.backend,
                        class = %c.backend_type().as_str(),
                        model = %c.upstream_model,
                        endpoint = %endpoint,
                        predicted_ms = %predicted,
                        "policy candidate"
                    );
                }
                if total > SHOWN {
                    tracing::trace!(
                        rule = rule_name.as_deref().unwrap_or("<unmatched>"),
                        elided = total - SHOWN,
                        n = total,
                        "more policy candidates not shown"
                    );
                }
            }

            match engine::evaluate(route, rule_name.as_deref(), &attrs, candidates) {
                engine::Outcome::Chosen { candidate, trace } => {
                    log_winner(&pref.label(), &candidate, &trace);
                    self.prom.record_policy_decision(if rule_name.is_some() {
                        "chosen"
                    } else {
                        "unmatched_served"
                    });
                    if let Some(n) = rule_name {
                        self.prom.record_policy_rule_match(n);
                    }
                    return Ok(selection(&mut scored, candidate));
                }
                engine::Outcome::Empty { trace } => {
                    tracing::debug!(
                        inference_pool = %pref.label(),
                        trace = %trace.summary(),
                        "no policy candidate survived"
                    );
                    if rule_name.is_some() && engine::falls_through(route) {
                        // FallThrough: try the next matching rule.
                        continue;
                    }
                    let (label, reason) = match rule_name {
                        Some(n) => (
                            "rejected_on_empty",
                            format!("every candidate was excluded by rule `{n}` (onEmpty: Reject)"),
                        ),
                        None => (
                            "unmatched_empty",
                            if any_rule_matched {
                                "every matching rule fell through and the unmatched route had \
                                 no candidate"
                            } else {
                                "no rule matched and the unmatched route had no candidate"
                            }
                            .to_string(),
                        ),
                    };
                    self.prom.record_policy_decision(label);
                    return Err(PickAndRecordOutcome::PolicyRejected { reason, model });
                }
            }
        }

        // Every matching rule fell through and there is no unmatched route to
        // close the plan: `unmatched: Reject`.
        self.prom.record_policy_decision("rejected_unmatched");
        Err(PickAndRecordOutcome::PolicyRejected {
            reason: "every matching rule fell through and unmatched=Reject".to_string(),
            model,
        })
    }

    /// Build one candidate per endpoint in a scored group.
    ///
    /// `group_idx` rides on `Candidate::score_idx` so the winner maps back to
    /// the group the critical section must commit with.
    fn candidates_for(
        &self,
        group: &ScoredGroup,
        group_idx: usize,
        dest: &kinference_policy::resolve::ResolvedWorkerSet,
        canonical_model: &str,
    ) -> Vec<kinference_policy::engine::Candidate> {
        let now = Instant::now();
        let ready_endpoints = group.pool.registry.len() as i64;
        let labels = Arc::new(dest.labels.clone());
        let model_names = model_names(
            &group.model,
            dest.aliases_for(&group.model),
            canonical_model,
        );
        // Per endpoint, since endpoints may disagree mid-rollout.
        let context_by_endpoint: HashMap<String, u32> = group
            .pool
            .registry
            .iter_snapshot()
            .into_iter()
            .filter_map(|(num, entry)| {
                let info = self.scoring.scrape.models(num);
                let window = info
                    .models
                    .iter()
                    .find(|m| m.name == group.model)
                    .and_then(|m| m.max_model_len)
                    .or_else(|| info.models.iter().filter_map(|m| m.max_model_len).min())?;
                Some((entry.score_addr, window))
            })
            .collect();
        // The two saturation signals per endpoint, as windowed means rather
        // than `WorkerScore`'s instantaneous queue, which spikes and clears.
        // Only scraped endpoints; the rest would read as all-zero (idle).
        let saturation_by_endpoint: HashMap<String, (f64, f64)> = group
            .pool
            .registry
            .iter_snapshot()
            .into_iter()
            .filter_map(|(num, entry)| {
                let m = self.scoring.scrape.sample(num)?;
                Some((
                    entry.score_addr,
                    (m.waiting_mean as f64, m.kv_cache_usage_mean as f64),
                ))
            })
            .collect();

        group
            .scores
            .iter()
            .map(|s| {
                let summaries = group.pool.inflight_prefixes.summaries_for(&s.endpoint);
                let predicted =
                    crate::picker::predicted_wait_ms(s, &group.pool.predictor, &summaries, now)
                        .total_ms();
                // Recorded for every candidate, as a `predictedTTFT` threshold
                // applies to every candidate; winners only would bias it low.
                self.prom
                    .record_predicted_ttft(&s.endpoint, &group.model, predicted as f64);
                let hit_ratio = if s.total_blocks > 0 {
                    s.matched_blocks as f64 / s.total_blocks as f64
                } else {
                    0.0
                };
                kinference_policy::engine::Candidate {
                    canonical_model: canonical_model.to_string(),
                    upstream_model: group.model.clone(),
                    backend: dest.name.clone(),
                    labels: labels.clone(),
                    target: kinference_policy::engine::CandidateTarget::Local {
                        pool_id: group.pool.id,
                        endpoint: s.endpoint.clone(),
                        score_idx: group_idx,
                        predicted_ttft_ms: predicted,
                    },
                    attrs: kinference_policy::predicate::CandidateAttrs {
                        context_window: context_by_endpoint.get(&s.endpoint).map(|v| *v as i64),
                        predicted_ttft_ms: Some(predicted as f64),
                        prefix_cache_hit_ratio: Some(hit_ratio),
                        // Absent, not zero, when the endpoint has not been
                        // scraped.
                        queued_prompts: saturation_by_endpoint.get(&s.endpoint).map(|(q, _)| *q),
                        kv_cache_usage: saturation_by_endpoint.get(&s.endpoint).map(|(_, k)| *k),
                        model_names: model_names.clone(),
                        backend_name: Some(dest.name.clone()),
                        backend_type: Some(
                            kinference_policy::crd::BackendType::Local
                                .as_str()
                                .to_string(),
                        ),
                        backend_ready_endpoints: Some(ready_endpoints),
                        // Provider-only.
                        backend_observed_ttft_ms: None,
                        backend_budget_exhausted: None,
                    },
                }
            })
            .collect()
    }
}

/// A destination that the router builds candidates for: a local worker set,
/// or a provider by namespace and name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CandidateDest {
    Local(kinference_policy::PoolId),
    Provider(String, String),
}

/// A candidate's `model.name`: `upstream`, then the `aliases` that its
/// destination declares for it, then `reached_by` if the destination serves
/// that name but does not declare it.
pub(super) fn model_names(upstream: &str, aliases: Vec<String>, reached_by: &str) -> Vec<String> {
    let mut names = vec![upstream.to_string()];
    for name in aliases
        .into_iter()
        .chain(std::iter::once(reached_by.to_string()))
    {
        if !names.contains(&name) {
            names.push(name);
        }
    }
    names
}

/// Add `name` to the candidates that the router already built for `key`,
/// unless they already contain it. Returns `false` if the router has built
/// nothing for `key` yet.
fn add_name(
    candidates: &mut [kinference_policy::engine::Candidate],
    built: &HashMap<(CandidateDest, String), Vec<usize>>,
    key: &(CandidateDest, String),
    name: &str,
) -> bool {
    let Some(indices) = built.get(key) else {
        return false;
    };
    for &i in indices {
        let names = &mut candidates[i].attrs.model_names;
        if !names.iter().any(|n| n == name) {
            names.push(name.to_string());
        }
    }
    true
}
