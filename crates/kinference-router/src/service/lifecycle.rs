//! The pick path behind `Pick()`: one request from body to chosen endpoint,
//! and the observation it leaves behind.
//!
//! `pick_and_record` is five steps, each a method with a typed result the
//! next consumes: parse, resolve the model, score the groups, select,
//! commit. Only `commit` writes state; the critical section under the
//! pool's `pick_lock` is inside it. What it reserves is released by the
//! `Observations` part, or by `abort_request` when the caller gives up.

use std::collections::HashMap;
use std::time::Instant;

use kinference_fleet::inflight::{BlockHashes, BlockQuery};

use super::*;
use crate::observations::PendingObservation;

/// What `Pick()` carried, read by every step of the pick.
struct PickRequest<'a> {
    /// The pushed body, forwarded unchanged unless a step rewrites it.
    body: &'a [u8],
    request_id: u64,
    client_request_id: Option<&'a str>,
    qos: Qos,
    pool_ref: Option<&'a kinference_policy::PoolRef>,
    identity: &'a kinference_policy::RequestIdentity,
}

/// Step 1. The body as a JSON tree, with what the later steps read from it.
struct Parsed {
    obj: serde_json::Value,
    /// The body's `model`, non-empty.
    model: String,
    facts: BodyFacts,
}

/// What the policy and the observation read from the body.
struct BodyFacts {
    streaming: bool,
    max_tokens: Option<i64>,
}

/// The body as `PullBody` will serve it. `None`: the pushed body goes out
/// unchanged, which the outcome spells as an empty body.
struct WireBody {
    rewritten: Option<Vec<u8>>,
}

impl WireBody {
    /// What `/render` and the engine see.
    fn bytes<'a>(&'a self, pushed: &'a [u8]) -> &'a [u8] {
        self.rewritten.as_deref().unwrap_or(pushed)
    }

    /// Name `model` in the body. `false` when the bytes are not a JSON
    /// object, which a body `parse` accepted cannot be.
    fn name_model(&mut self, pushed: &[u8], model: &str) -> bool {
        match crate::pick_body::rewrite_model(self.bytes(pushed), model) {
            Some(bytes) => {
                self.rewritten = Some(bytes);
                true
            }
            None => false,
        }
    }

    /// For the outcome: the rewritten bytes, or empty.
    fn into_bytes(self) -> Vec<u8> {
        self.rewritten.unwrap_or_default()
    }
}

/// Step 2. The upstream model, the pools serving it, the body as the engine
/// will see it, and what scoring consumes.
struct Resolved {
    model: String,
    /// The name that the client asked for, before the router resolved any alias.
    requested_model: String,
    /// Empty when only a provider in the named pool serves the model.
    serving: Vec<Arc<Pool>>,
    body: WireBody,
    facts: BodyFacts,
    inputs: ScoreInput,
}

/// Step 3. The groups to choose among.
struct Scored {
    model: String,
    requested_model: String,
    body: WireBody,
    facts: BodyFacts,
    groups: Vec<ScoredGroup>,
    /// Why the first pool's pick lost its prefix signal, for the outcome.
    degraded: Option<String>,
    /// The inputs again, for the policy to score more groups. `None` when
    /// nothing could need them.
    spare: Option<ScoreInput>,
}

/// Step 4. What `commit` applies.
enum Selected {
    /// A group, and the policy's endpoint in it when it chose one; otherwise
    /// the picker chooses, under the pool's lock.
    Local {
        group: ScoredGroup,
        endpoint: Option<String>,
        body: WireBody,
        streaming: bool,
        degraded: Option<String>,
    },
    /// An off-cluster provider. The body names the vendor's model; nothing
    /// pool-scoped applies.
    Provider {
        pick: ProviderPick,
        body: Vec<u8>,
        streaming: bool,
    },
}

impl KinferenceService {
    /// The full pick path behind gRPC `Pick()`. A refusal returns from the
    /// step that found it and writes no state.
    ///
    /// The caller mints `request_id` (see `mint_response_id`), so the same id
    /// flows through pending-observation lookup and log correlation.
    pub(crate) async fn pick_and_record(
        &self,
        body: &[u8],
        request_id: u64,
        client_request_id: Option<&str>,
        qos: Qos,
        pool_ref: Option<&kinference_policy::PoolRef>,
        identity: &kinference_policy::RequestIdentity,
    ) -> PickAndRecordOutcome {
        let req = PickRequest {
            body,
            request_id,
            client_request_id,
            qos,
            pool_ref,
            identity,
        };

        let parsed = match Self::parse(&req) {
            Ok(p) => p,
            Err(out) => return out,
        };
        let resolved = match self.resolve_model(&req, parsed) {
            Ok(r) => r,
            Err(out) => return out,
        };
        tracing::Span::current().record("model", resolved.model.as_str());
        let scored = self.score_groups(&req, resolved).await;
        let selected = match self.select(&req, scored).await {
            Ok(s) => s,
            Err(out) => return out,
        };
        self.commit(&req, selected)
    }

    /// Step 1. `Err` is a `BadRequest`: not JSON, not an object, or no
    /// `model`.
    fn parse(req: &PickRequest<'_>) -> Result<Parsed, PickAndRecordOutcome> {
        // A JSON tree, rewritten in place later (see `crate::pick_body`).
        // Not a typed struct, which would drop unknown fields.
        let obj: serde_json::Value = match serde_json::from_slice(req.body) {
            Ok(v) => v,
            Err(e) => {
                return Err(PickAndRecordOutcome::BadRequest {
                    reason: format!("body is not a valid OpenAI ChatCompletionRequest: {e}"),
                    model: String::new(),
                });
            }
        };
        if !obj.is_object() {
            return Err(PickAndRecordOutcome::BadRequest {
                reason: "body is not a valid OpenAI ChatCompletionRequest: expected a JSON object"
                    .to_string(),
                model: String::new(),
            });
        }

        // The body is the only source for `model`.
        let model = obj.get("model").and_then(|v| v.as_str()).unwrap_or("");
        if model.is_empty() {
            return Err(PickAndRecordOutcome::BadRequest {
                reason: "request body has an empty or absent `model`".to_string(),
                model: String::new(),
            });
        }
        let facts = BodyFacts {
            // Whether the first-byte report will be a real TTFT, for
            // `Observations::finalize`. The same coercion as the body rewrite;
            // see [`crate::pick_body::stream_flag`].
            streaming: crate::pick_body::stream_flag(&obj),
            // For `maxTokens` predicates and context-window fitting.
            max_tokens: obj.get("max_tokens").and_then(serde_json::Value::as_i64),
        };
        Ok(Parsed {
            model: model.to_string(),
            facts,
            obj,
        })
    }

    /// Step 2. Resolve `auto` or a declared alias to the upstream model and
    /// the pools serving it, write it into the body, and take the chat inputs
    /// out of the tree. `Err` is `NoWorkers`: no model known for `auto`, or
    /// no pool and no provider serves the model.
    fn resolve_model(
        &self,
        req: &PickRequest<'_>,
        parsed: Parsed,
    ) -> Result<Resolved, PickAndRecordOutcome> {
        let Parsed {
            mut obj,
            mut model,
            facts,
        } = parsed;

        // `auto`: choose a concrete model, then route normally. `force_model`
        // drives the body rewrite below. With no models known yet, fall through
        // to NoWorkers.
        let mut force_model: Option<String> = None;
        if model == AUTO_MODEL {
            let messages = obj.get("messages").and_then(|v| v.as_array());
            match self
                .scoring
                .choose_model_for_auto(messages.map(|m| m.as_slice()).unwrap_or(&[]))
            {
                Some(chosen) => {
                    tracing::debug!(to_model = %chosen, "resolved the auto model");
                    model = chosen.clone();
                    force_model = Some(chosen);
                }
                None => {
                    tracing::debug!("no model to resolve auto to");
                    return Err(PickAndRecordOutcome::NoWorkers { model });
                }
            }
        }

        // Save the name that the client sent, since the code below may rewrite
        // `model` to an upstream. The policy driver generates candidates for
        // the saved name.
        let requested_model = model.clone();

        // Several pools may serve the model; they are scored and ranked against
        // each other.
        let mut serving = self.scoring.resolve_pools(&model);

        // Alias fallback. The model index only holds names engines report, so
        // translate a declared alias via the named pool's worker sets (requires
        // policy and a named pool). The first served upstream only decides
        // which group the router scores first: under a policy, every
        // destination that declares the name stays a candidate.
        if serving.is_empty()
            && let Some(upstream) = self.resolve_alias(req.pool_ref, &model)
        {
            tracing::debug!(
                alias = %model,
                to_model = %upstream,
                "resolved an alias"
            );
            // Drives the body rewrite below, as for `auto`.
            force_model = Some(upstream.clone());
            model = upstream;
            serving = self.scoring.resolve_pools(&model);
        }

        // A model no local pool serves may still be served by a provider in
        // the named pool, so check before refusing.
        if serving.is_empty() {
            if !self.provider_serves(req.pool_ref, &model) {
                tracing::debug!(model = %model, "no worker set serves the model");
                return Err(PickAndRecordOutcome::NoWorkers { model });
            }
            tracing::debug!(model = %model, "only a provider serves the model");
        }

        // The Pick-time body rewrites: the resolved model, and
        // `stream_options.include_usage` for streaming requests. Only a changed
        // body is re-serialized.
        let mutated = crate::pick_body::apply_pick_body_mutations(&mut obj, force_model.as_deref());
        let body = WireBody {
            rewritten: mutated.then(|| {
                serde_json::to_vec(&obj).expect("re-serializing a parsed JSON body cannot fail")
            }),
        };

        // The chat-template inputs, taken out of the tree now that it is
        // serialized. `/render` gets the outbound bytes so it sees what the
        // engine will.
        let messages = match obj.get_mut("messages").map(serde_json::Value::take) {
            Some(serde_json::Value::Array(v)) => v,
            _ => Vec::new(),
        };
        let tools = match obj.get_mut("tools").map(serde_json::Value::take) {
            Some(serde_json::Value::Array(v)) => v,
            _ => Vec::new(),
        };
        let tool_choice = obj
            .get_mut("tool_choice")
            .map(serde_json::Value::take)
            .filter(|v| !v.is_null());
        let add_generation_prompt = obj
            .get("add_generation_prompt")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true);
        let chat_extras = ChatExtras {
            tools,
            tool_choice,
            add_generation_prompt,
        };
        let inputs = ScoreInput {
            input: Input::Messages(messages),
            chat_extras,
            raw_body: Some(body.bytes(req.body).to_vec()),
        };
        Ok(Resolved {
            model,
            requested_model,
            serving,
            body,
            facts,
            inputs,
        })
    }

    /// Step 3. Score the first serving pool, then the rest with the first's
    /// tokens. Records `score_degraded`. Never refuses: a pool with no ready
    /// worker yields no group, and a policy may still place the request.
    async fn score_groups(&self, req: &PickRequest<'_>, resolved: Resolved) -> Scored {
        let Resolved {
            model,
            requested_model,
            serving,
            body,
            facts,
            inputs,
        } = resolved;

        // Scoring consumes the inputs, but a second pool or a policy may need
        // them again; only clone in that case.
        let spare = (self.policy_for(req).is_some() || serving.len() > 1).then(|| inputs.clone());
        let mut degraded: Option<String> = None;
        let mut groups: Vec<ScoredGroup> = Vec::new();
        if let Some(first) = serving.first() {
            let (outcome, score_side) = self
                .scoring
                .score_workers(&*self.tokenizer, first, &model, inputs, HashMap::new())
                .await;
            match outcome {
                ScoreOutcome::Ok(resp) => groups.push(ScoredGroup {
                    pool: first.clone(),
                    model: model.clone(),
                    scores: resp.scores,
                    side: score_side,
                }),
                ScoreOutcome::Unavailable(resp, why) => {
                    self.prom.record_score_degraded(why.as_label());
                    degraded = Some(why.reason().to_string());
                    groups.push(ScoredGroup {
                        pool: first.clone(),
                        model: model.clone(),
                        scores: resp.scores,
                        side: score_side,
                    });
                }
                // Not fatal: another pool or a provider may serve the model.
                ScoreOutcome::NoWorkers => {}
            }
        }

        // The remaining pools serving this model, reusing the first group's
        // tokens.
        if serving.len() > 1 {
            let reuse = groups
                .first()
                .map(|g| g.side.tokens.clone())
                .filter(|t| !t.is_empty());
            for pool in &serving[1..] {
                let Some(spare) = spare.as_ref() else {
                    continue;
                };
                let input = match reuse.clone() {
                    Some(t) => ScoreInput::tokens(t),
                    None => spare.clone(),
                };
                let (outcome, side) = self
                    .scoring
                    .score_workers(&*self.tokenizer, pool, &model, input, HashMap::new())
                    .await;
                let scores = match outcome {
                    ScoreOutcome::Ok(r) => r.scores,
                    ScoreOutcome::Unavailable(r, why) => {
                        self.prom.record_score_degraded(why.as_label());
                        r.scores
                    }
                    ScoreOutcome::NoWorkers => continue,
                };
                if scores.is_empty() {
                    continue;
                }
                groups.push(ScoredGroup {
                    pool: pool.clone(),
                    model: model.clone(),
                    scores,
                    side,
                });
            }
        }
        Scored {
            model,
            requested_model,
            body,
            facts,
            groups,
            degraded,
            spare,
        }
    }

    /// Step 4. Rank the groups, run the policy when one applies, and name the
    /// chosen model in the body when it differs. `Err` is `NoWorkers`,
    /// `PolicyRejected` or a `BadRequest` for a body that could not be
    /// rewritten; no state is written.
    async fn select(
        &self,
        req: &PickRequest<'_>,
        scored: Scored,
    ) -> Result<Selected, PickAndRecordOutcome> {
        let Scored {
            model,
            requested_model,
            mut body,
            facts,
            groups,
            degraded,
            spare,
        } = scored;
        let streaming = facts.streaming;

        // Rank across the pools serving this model, choosing the pool; the
        // picker chooses the endpoint under that pool's lock. `None` isn't yet
        // fatal, since a policy may still find something.
        //
        // Predictions are recorded by exactly one ranker per request: the
        // policy's `candidates_for` when a policy is in play, otherwise this.
        // Recording in both would double-count. A request naming an unresolved
        // pool records none, but is counted under
        // `kinference_policy_decisions_total`.
        let policy = self.policy_for(req);
        let winner = best_group(groups, policy.is_none().then_some(&*self.prom));

        // No pool named, or policy disabled: the ranking decided, and providers
        // are unreachable.
        let Some((pref, store)) = policy else {
            let Some(group) = winner else {
                tracing::debug!("no ready worker serves the model");
                return Err(PickAndRecordOutcome::NoWorkers { model });
            };
            return Ok(Selected::Local {
                group,
                endpoint: None,
                body,
                streaming,
                degraded,
            });
        };

        // Policy. Candidates may span pools, each with its own `pick_lock`, so
        // ranking is lock-free (avoiding lock-order deadlocks) and the winner
        // commits under its own lock. The ranking input may be one concurrent
        // pick stale.
        let attrs = kinference_policy::predicate::RequestAttrs {
            // Upstream first, then its declared aliases.
            model: {
                let mut names = vec![model.clone()];
                names.extend(self.declared_aliases(req.pool_ref, &model));
                names
            },
            requested_model,
            // Block-granular, from scoring. `None` when nothing local was
            // scored, so `inputTokens` predicates and context-window fitting
            // don't apply.
            input_tokens: winner.as_ref().and_then(|w| {
                w.scores
                    .first()
                    .map(|s| (s.total_blocks as i64) * (w.pool.block_size as i64))
            }),
            max_tokens: facts.max_tokens,
            qos: Some(req.qos.as_label().to_string()),
            tenant: req.identity.tenant.clone(),
            // Nothing produces tags yet.
            tag: Vec::new(),
        };
        // Re-bind to the winning group, whose pool's scores and prefix hashes
        // the critical section uses.
        let (group, endpoint) = match self
            .policy_select(pref, store, winner, &model, spare, attrs)
            .await
        {
            // An off-cluster destination: nothing pool-scoped applies. Rewrite
            // the body to the vendor's model name; `commit` holds an observation
            // for the reported TTFT.
            Ok(PolicySelection::Provider(pick)) => {
                if !body.name_model(req.body, &pick.upstream_model) {
                    tracing::error!(
                        to_model = %pick.upstream_model,
                        provider = %pick.provider.label(),
                        "could not rewrite the body for the provider; refusing"
                    );
                    return Err(PickAndRecordOutcome::BadRequest {
                        reason: "could not rewrite body model for the chosen provider".to_string(),
                        model,
                    });
                }
                tracing::debug!(
                    provider = %pick.provider.label(),
                    backend = %pick.backend_ref.label(),
                    to_model = %pick.upstream_model,
                    "routing to a provider"
                );
                return Ok(Selected::Provider {
                    pick,
                    body: body.into_bytes(),
                    streaming,
                });
            }
            Ok(PolicySelection::Local { group, endpoint }) => (group, Some(endpoint)),
            Ok(PolicySelection::Picker { group: Some(group) }) => (group, None),
            Ok(PolicySelection::Picker { group: None }) => {
                // Nothing local was scored and no provider chosen.
                tracing::debug!("policy chose no destination");
                return Err(PickAndRecordOutcome::NoWorkers { model });
            }
            Err(outcome) => return Err(outcome),
        };
        // The policy may have chosen a different model than the body names, so
        // rewrite it.
        if group.model != model {
            if !body.name_model(req.body, &group.model) {
                // Unreachable for a body we already parsed once. Refuse rather
                // than forward a body naming a model the chosen endpoint does
                // not serve.
                tracing::error!(
                    to_model = %group.model,
                    "could not rewrite the body for the chosen model; refusing"
                );
                return Err(PickAndRecordOutcome::BadRequest {
                    reason: "could not rewrite body model for the chosen candidate".to_string(),
                    model: group.model,
                });
            }
            tracing::debug!(
                to_model = %group.model,
                "rewriting the body's model"
            );
        }
        Ok(Selected::Local {
            group,
            endpoint,
            body,
            streaming,
            degraded,
        })
    }

    /// Step 5. Apply the pick. Local: the critical section under the pool's
    /// `pick_lock`, then the pending stash and the routed counter. Provider:
    /// the label stash and the routed counter. Only here is state written.
    fn commit(&self, req: &PickRequest<'_>, selected: Selected) -> PickAndRecordOutcome {
        let (group, forced_endpoint, body, streaming, degraded) = match selected {
            Selected::Local {
                group,
                endpoint,
                body,
                streaming,
                degraded,
            } => (group, endpoint, body, streaming, degraded),
            Selected::Provider {
                pick,
                body,
                streaming,
            } => {
                self.observations.record_provider_pick(
                    req.request_id,
                    &pick.provider,
                    &pick.upstream_model,
                    req.qos,
                    &req.identity.tenant,
                    streaming,
                );
                self.prom.record_routed(
                    &req.identity.tenant,
                    &pick.provider.name,
                    kinference_policy::crd::BackendType::Provider,
                );
                return PickAndRecordOutcome::Provider {
                    pick,
                    response_id: req.request_id,
                    body,
                };
            }
        };
        let ScoredGroup {
            pool,
            model,
            scores,
            side: score_side,
        } = group;

        // Derived AFTER the policy may have re-bound `pool`.
        let predictor_cfg = &pool.predictor;
        let picker_cfg = &pool.picker;
        let inflight = &*pool.inflight;
        let pick_lock = &*pool.pick_lock;
        let inflight_enabled = picker_cfg.inflight_tracking;
        // Built before the lock: the critical section only reads them.
        let mut query_hashes: HashMap<u32, BlockQuery> = score_side
            .hashes_by_block_size
            .iter()
            .map(|(size, hashes)| (*size, BlockQuery::new(BlockHashes::new(hashes.clone()))))
            .collect();
        let no_query = BlockQuery::default();

        // The critical section: pick and state mutation.
        let chosen_endpoint: String;
        let pending: PendingObservation;
        {
            // Recover from poisoning: one panic would otherwise break every
            // later pick on this pool. The mutex guards `()`, only ordering the
            // section, and the state it touches is TTL-swept.
            let _pick_guard = pick_lock.lock().unwrap_or_else(|poisoned| {
                tracing::warn!(
                    workerset = %pool.name,
                    "pick lock poisoned by an earlier panic; continuing"
                );
                poisoned.into_inner()
            });

            let mut scores = scores;
            let kv_blocks_per_worker: Vec<u32> = scores.iter().map(|s| s.matched_blocks).collect();
            let expected_overlap_alpha = pool.predictor.expected_overlap_alpha;
            let overlaps_per_worker: Vec<Vec<kinference_fleet::inflight::DispatchOverlap>> = scores
                .iter()
                .map(|s| {
                    let query = query_hashes.get(&s.block_size).unwrap_or(&no_query);
                    pool.inflight_prefixes.overlaps_for(&s.endpoint, query)
                })
                .collect();
            let inflight_workload_per_worker: Vec<
                Vec<kinference_fleet::inflight::InflightSummary>,
            > = overlaps_per_worker
                .iter()
                .map(|d| d.iter().map(|o| o.summary).collect())
                .collect();
            let per_worker_dispatch_overlap: Vec<Vec<f32>> = overlaps_per_worker
                .iter()
                .map(|d| d.iter().map(|o| o.overlap_blocks as f32).collect())
                .collect();
            let inflight_per_worker: Vec<u32> = if inflight_enabled {
                scores.iter().map(|s| inflight.get(&s.endpoint)).collect()
            } else {
                vec![0; scores.len()]
            };

            let score_now = Instant::now();
            let per_worker_dispatch_progress: Vec<Vec<f32>> = inflight_workload_per_worker
                .iter()
                .map(|dispatches| {
                    let capacity = predictor_cfg.inflight_concurrent_capacity;
                    dispatches
                        .iter()
                        .enumerate()
                        .map(|(idx, d)| {
                            let position = (idx as u32).saturating_add(1);
                            if position > capacity {
                                return 0.0;
                            }
                            let est = crate::picker::prefill_ms_for(
                                predictor_cfg,
                                d.uncached_at_dispatch,
                                d.total_at_dispatch,
                            );
                            if est <= 0.0 {
                                return 1.0;
                            }
                            let elapsed = score_now
                                .checked_duration_since(d.dispatched_at)
                                .map(|d| d.as_millis() as f32)
                                .unwrap_or(0.0);
                            (elapsed / est).clamp(0.0, 1.0)
                        })
                        .collect()
                })
                .collect();

            for (i, s) in scores.iter_mut().enumerate() {
                let scaled = crate::picker::scale_per_dispatch_by_progress(
                    &per_worker_dispatch_overlap[i],
                    &per_worker_dispatch_progress[i],
                );
                let layered = crate::picker::compute_layered_overlap(
                    &scaled,
                    kv_blocks_per_worker[i] as f32,
                    expected_overlap_alpha,
                );
                s.matched_blocks = layered.round() as u32;
            }
            let inputs = crate::picker::PickInputs {
                scores: &scores,
                predictor: predictor_cfg,
                picker: picker_cfg,
                inflight: &inflight_per_worker,
                inflight_workload: &inflight_workload_per_worker,
                kv_blocks: &kv_blocks_per_worker,
                now: score_now,
            };

            // The policy already chose, in `select`.
            let forced_pick = forced_endpoint.as_deref().and_then(|ep| {
                let found = scores.iter().position(|s| s.endpoint == ep).map(|index| {
                    crate::picker::PickResult {
                        index,
                        worker: &scores[index],
                        replication_fired: false,
                    }
                });
                if found.is_none() {
                    // Should be unreachable, since the endpoint came from
                    // this group's scores. Warn, since the decision counter
                    // already recorded `chosen`.
                    tracing::warn!(
                        workerset = %pool.name,
                        endpoint = %ep,
                        scored = scores.len(),
                        "policy chose an unscored endpoint; using the picker"
                    );
                    self.prom.record_policy_decision("forced_endpoint_missing");
                }
                found
            });

            let pick_result = match forced_pick {
                Some(r) => r,
                None => match crate::picker::pick_endpoint(&inputs) {
                    Some(r) => r,
                    None => {
                        tracing::warn!(scored = scores.len(), "picker found no eligible worker");
                        return PickAndRecordOutcome::BadRequest {
                            reason: "picker returned no eligible worker".to_string(),
                            model: model.clone(),
                        };
                    }
                },
            };
            let chosen = pick_result.worker;
            let chosen_idx = pick_result.index;
            let replication_fired = pick_result.replication_fired;

            if tracing::enabled!(tracing::Level::TRACE) {
                tracing::trace!(
                    table = %crate::picker::format_score_table(
                        &inputs,
                        expected_overlap_alpha,
                        Some(&chosen.endpoint),
                    ),
                    "score table"
                );
            }
            tracing::debug!(
                endpoint = %chosen.endpoint,
                worker_id = %chosen.name,
                scored = scores.len(),
                "picked an endpoint"
            );

            let chosen_inflight = inflight_per_worker[chosen_idx];
            let chosen_kv_blocks = kv_blocks_per_worker[chosen_idx];
            let chosen_workload: &[kinference_fleet::inflight::InflightSummary] =
                &inflight_workload_per_worker[chosen_idx];
            let chosen_q_breakdown = crate::picker::compute_queue_cost(
                chosen,
                predictor_cfg,
                chosen_workload,
                score_now,
            );
            let chosen_overlap_per_dispatch: Vec<u32> = per_worker_dispatch_overlap[chosen_idx]
                .iter()
                .map(|f| *f as u32)
                .collect();
            let prediction =
                crate::picker::predicted_wait_ms(chosen, predictor_cfg, chosen_workload, score_now);
            // Prefix-cache outcome for the pick we actually made. Skipped when
            // the pick was degraded: a tokenize failure would look like a
            // genuine 0% cache hit, and a prefix-index failure can report
            // matched blocks against a zero-length prompt. Those picks are
            // already counted by `kinference_score_degraded_total{reason}`.
            //
            // `chosen.matched_blocks` is the layered value (it was overwritten
            // above with radix match plus in-flight prefill credit), while
            // `chosen_kv_blocks` is the raw tree match for the same pick.
            if degraded.is_none() {
                self.prom.record_prefix_match(
                    &model,
                    req.qos.as_label(),
                    chosen.total_blocks,
                    chosen.matched_blocks,
                    chosen_kv_blocks,
                );
            }

            let chosen_clone = chosen.clone();
            pending = PendingObservation {
                request_id: req.request_id,
                workerset: pool.name.clone(),
                profile: pool.profile.clone(),
                inflight: crate::observations::InflightHandles::of(&pool),
                picked_at: score_now,
                chosen_worker: chosen_clone,
                prediction,
                q_inflight: chosen_inflight,
                kv_blocks: chosen_kv_blocks,
                inflight_workload_blocks: chosen_q_breakdown.inflight_workload_blocks,
                q_ms_constant: chosen_q_breakdown.q_ms_constant,
                q_ms_workload: chosen_q_breakdown.q_ms_workload,
                inflight_overlap_blocks: chosen_overlap_per_dispatch,
                expected_overlap_alpha,
                predictor_cfg: predictor_cfg.clone(),
                picker_cfg: picker_cfg.clone(),
                external_request_id: req.client_request_id.map(|s| s.to_string()),
                replication_fired,
                model: model.clone(),
                streaming,
                qos: req.qos,
                tenant: req.identity.tenant.clone(),
            };
            // For ReportFinalToken; the record above is consumed earlier, at
            // ReportFirstToken.
            self.observations.labels.insert(
                req.request_id,
                RequestMeta {
                    model: model.clone(),
                    qos: req.qos,
                    tenant: req.identity.tenant.clone(),
                    streaming,
                    picked_at: score_now,
                    // A local pick, so there is no provider to attribute
                    // feedback to.
                    provider: None,
                },
            );

            if inflight_enabled {
                inflight.inc(&chosen.endpoint);
            }
            let chosen_hashes = query_hashes
                .remove(&chosen.block_size)
                .map(BlockQuery::into_hashes)
                .unwrap_or_default();
            let chosen_uncached = chosen.total_blocks.saturating_sub(chosen.matched_blocks);
            pool.inflight_prefixes.insert(
                req.request_id,
                chosen.endpoint.clone(),
                chosen_hashes,
                chosen_uncached,
                chosen.total_blocks,
            );

            chosen_endpoint = chosen.endpoint.clone();
        }

        self.observations.pending.insert(pending);
        // The bare name, as `backend.name` spells it; a CRD worker set's
        // `pool.name` is `namespace/name`.
        self.prom.record_routed(
            &req.identity.tenant,
            &pool.key.name,
            kinference_policy::crd::BackendType::Local,
        );

        match degraded {
            Some(reason) => PickAndRecordOutcome::Degraded {
                endpoint: chosen_endpoint,
                response_id: req.request_id,
                reason,
                body: body.into_bytes(),
                model,
            },
            None => PickAndRecordOutcome::Ok {
                endpoint: chosen_endpoint,
                response_id: req.request_id,
                body: body.into_bytes(),
                model,
            },
        }
    }

    /// The named pool and the policy store, when both exist: only then does a
    /// policy rank.
    fn policy_for<'a>(
        &'a self,
        req: &PickRequest<'a>,
    ) -> Option<(
        &'a kinference_policy::PoolRef,
        &'a Arc<kinference_policy::store::PolicyStore>,
    )> {
        Some((req.pool_ref?, self.policy.as_ref()?))
    }

    /// Release everything held for `response_id` because the request will never
    /// complete. Backs the `Abort` RPC.
    ///
    /// Returns `(released, bytes_released)`. `released` is false only for an
    /// unknown id or a duplicate abort; never an error, since `Abort` is
    /// idempotent.
    ///
    /// Releases the inflight dispatch for both abort reasons, since after
    /// `PULL_FAILED` no `ReportFirstToken` is coming either.
    pub(crate) fn abort_request(&self, response_id: u64) -> (bool, u64) {
        let (body_released, bytes) = self.bodies.store.abort(response_id);
        let obs_released = self.observations.abort(response_id);
        self.observations.labels.take(response_id);
        (body_released || obs_released, bytes)
    }
}
