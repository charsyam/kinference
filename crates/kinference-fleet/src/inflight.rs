//! In-flight state: work dispatched to a worker and not yet observed.
//!
//! Two per-endpoint trackers with the same lifecycle and endpoint key
//! (`score_addr`): [`InflightRegistry`] counts picks not yet observed as
//! first-token; [`InflightPrefixes`] remembers the prompt prefixes whose
//! `BlockStored` events have not landed yet.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;

/// Per-endpoint count of picks not yet observed as first-token.
///
/// Scraped metrics lag, so during a burst several picks can land before a
/// scrape reflects the first. The picker uses this count to break ties and
/// avoid piling a burst onto one worker.
///
/// Keyed by `endpoint` (`host:port`), which every `WorkerScore` carries. A
/// dead endpoint's entry just becomes an unused map slot.
#[derive(Default)]
pub struct InflightRegistry {
    counts: DashMap<String, AtomicU32>,
}

impl InflightRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bump the in-flight count for an endpoint, after a pick.
    pub fn inc(&self, endpoint: &str) {
        self.counts
            .entry(endpoint.to_string())
            .or_insert_with(|| AtomicU32::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the in-flight count for an endpoint, saturating at zero.
    /// Called when the pick's observation is released.
    pub fn dec(&self, endpoint: &str) {
        if let Some(entry) = self.counts.get(endpoint) {
            // Saturate at 0, where `fetch_sub` would wrap.
            let atom = entry.value();
            loop {
                let current = atom.load(Ordering::Relaxed);
                if current == 0 {
                    return;
                }
                if atom
                    .compare_exchange_weak(
                        current,
                        current - 1,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    return;
                }
            }
        }
    }

    /// Current in-flight count for an endpoint; 0 if never seen.
    pub fn get(&self, endpoint: &str) -> u32 {
        self.counts
            .get(endpoint)
            .map(|e| e.value().load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}

/// A prompt's block hashes, sorted and deduplicated, so that an overlap is a
/// merge of two sorted runs instead of one hash lookup per block.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BlockHashes(Vec<u64>);

impl BlockHashes {
    pub fn new(mut hashes: Vec<u64>) -> Self {
        hashes.sort_unstable();
        hashes.dedup();
        Self(hashes)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl FromIterator<u64> for BlockHashes {
    fn from_iter<I: IntoIterator<Item = u64>>(iter: I) -> Self {
        Self::new(iter.into_iter().collect())
    }
}

/// A pick's block hashes in two forms, built once before the pick lock, so
/// that each dispatch's overlap can take whichever walk is cheapest.
#[derive(Debug, Clone, Default)]
pub struct BlockQuery {
    hashes: BlockHashes,
    set: HashSet<u64>,
}

impl BlockQuery {
    pub fn new(hashes: BlockHashes) -> Self {
        let set = hashes.0.iter().copied().collect();
        Self { hashes, set }
    }

    /// The sorted run, for recording the dispatch.
    pub fn into_hashes(self) -> BlockHashes {
        self.hashes
    }
}

impl FromIterator<u64> for BlockQuery {
    fn from_iter<I: IntoIterator<Item = u64>>(iter: I) -> Self {
        Self::new(iter.into_iter().collect())
    }
}

/// [`InflightDispatch::overlap`] looks the dispatch's hashes up in the query's
/// set when the query is at least this many times longer.
const LOOKUP_RATIO: usize = 8;
/// [`InflightDispatch::overlap`] scans the dispatch for each query hash when
/// the dispatch is at least this many times longer.
const SEARCH_RATIO: usize = 4;
/// A scan jumps ahead to its guess only when the guess is this far ahead.
const JUMP_GAP: usize = 64;
/// Entries a scan steps over, and then counts, at a time.
const SCAN_BLOCK: usize = 16;

/// One outstanding dispatch. Bit `i` of `stored` is set once the `BlockStored`
/// event for `hashes[i]` lands; `remaining` counts the unset bits, and the
/// dispatch is dropped once it reaches zero.
///
/// `uncached_at_dispatch` (blocks the engine had to prefill) is stored since
/// the radix tree will have moved on by the time it's needed.
/// `total_at_dispatch` is the full prompt's block count, for the quadratic
/// prefill cost model.
#[derive(Debug, Clone)]
struct InflightDispatch {
    endpoint: String,
    hashes: BlockHashes,
    stored: Vec<u64>,
    remaining: usize,
    dispatched_at: Instant,
    uncached_at_dispatch: u32,
    total_at_dispatch: u32,
}

impl InflightDispatch {
    fn is_stored(&self, i: usize) -> bool {
        (self.stored[i / 64] >> (i % 64)) & 1 == 1
    }

    fn summary(&self) -> InflightSummary {
        InflightSummary {
            dispatched_at: self.dispatched_at,
            uncached_at_dispatch: self.uncached_at_dispatch,
            total_at_dispatch: self.total_at_dispatch,
        }
    }

    /// How many of `query`'s blocks this dispatch has yet to store.
    ///
    /// The cheapest walk depends on how the two lengths compare: a hash lookup
    /// per dispatch block when the dispatch is much shorter, a forward scan
    /// per query block when the query is much shorter, and a merge of the two
    /// sorted runs otherwise. The ratios come from benchmarks with random
    /// 64-bit hashes.
    fn overlap(&self, query: &BlockQuery) -> u32 {
        let (d, q) = (self.hashes.len(), query.hashes.len());
        if d.saturating_mul(LOOKUP_RATIO) <= q {
            self.overlap_by_lookup(&query.set)
        } else if q.saturating_mul(SEARCH_RATIO) <= d {
            self.overlap_by_search(&query.hashes.0)
        } else {
            self.overlap_by_merge(&query.hashes.0)
        }
    }

    fn overlap_by_lookup(&self, query: &HashSet<u64>) -> u32 {
        let mut n = 0u32;
        for (i, h) in self.hashes.0.iter().enumerate() {
            n += (query.contains(h) && !self.is_stored(i)) as u32;
        }
        n
    }

    /// The search scans forward in blocks and counts smaller entries without
    /// branching. AVX2 counts full blocks on supported x86-64 CPUs.
    /// Uniform hashes permit a jump to `y * len / 2^64` when the preceding
    /// hash is smaller than `y`. The scan handles clustered hashes in order.
    fn overlap_by_search(&self, query: &[u64]) -> u32 {
        #[cfg(target_arch = "x86_64")]
        if std::arch::is_x86_feature_detected!("avx2") {
            // Feature detection guards every call to the AVX2 implementation.
            return unsafe { self.overlap_by_search_avx2(query) };
        }
        self.overlap_by_search_with(query, |block, y| {
            block.iter().map(|&x| (x < y) as usize).sum()
        })
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2")]
    unsafe fn overlap_by_search_avx2(&self, query: &[u64]) -> u32 {
        self.overlap_by_search_with(query, |block, y| unsafe { count_less_avx2(block, y) })
    }

    #[inline(always)]
    fn overlap_by_search_with(&self, query: &[u64], count: impl Fn(&[u64], u64) -> usize) -> u32 {
        let a = &self.hashes.0;
        let (mut lo, mut n) = (0usize, 0u32);
        for &y in query {
            let guess = ((y as u128 * a.len() as u128) >> 64) as usize;
            if guess > lo + JUMP_GAP && a[guess - 1] < y {
                lo = guess;
            }
            while lo + SCAN_BLOCK <= a.len() && a[lo + SCAN_BLOCK - 1] < y {
                lo += SCAN_BLOCK;
            }
            let end = (lo + SCAN_BLOCK).min(a.len());
            lo += count(&a[lo..end], y);
            if lo == a.len() {
                break;
            }
            n += (a[lo] == y && !self.is_stored(lo)) as u32;
        }
        n
    }

    /// Branchless: block hashes are uniformly random, so a branch on the
    /// comparison mispredicts about half the time.
    fn overlap_by_merge(&self, b: &[u64]) -> u32 {
        let a = &self.hashes.0;
        let (mut i, mut j, mut n) = (0usize, 0usize, 0u32);
        while i < a.len() && j < b.len() {
            let (x, y) = (a[i], b[j]);
            n += (x == y && !self.is_stored(i)) as u32;
            i += (x <= y) as usize;
            j += (x >= y) as usize;
        }
        n
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn count_less_avx2(block: &[u64], y: u64) -> usize {
    use std::arch::x86_64::*;

    if block.len() != SCAN_BLOCK {
        return block.iter().map(|&x| (x < y) as usize).sum();
    }
    let sign = _mm256_set1_epi64x(i64::MIN);
    let bound = _mm256_xor_si256(_mm256_set1_epi64x(y as i64), sign);
    let mut counts = _mm256_setzero_si256();
    for offset in [0, 4, 8, 12] {
        // The length check guarantees four readable values at each offset.
        let values = unsafe { _mm256_loadu_si256(block.as_ptr().add(offset).cast()) };
        // Flipping the sign bit maps unsigned order to signed order.
        let less = _mm256_cmpgt_epi64(bound, _mm256_xor_si256(values, sign));
        counts = _mm256_sub_epi64(counts, less);
    }
    let halves = _mm_add_epi64(
        _mm256_castsi256_si128(counts),
        _mm256_extracti128_si256::<1>(counts),
    );
    let sum = _mm_add_epi64(halves, _mm_unpackhi_epi64(halves, halves));
    _mm_cvtsi128_si64(sum) as usize
}

/// One outstanding dispatch, without its hashes, for the picker's queue-cost
/// model.
#[derive(Debug, Clone, Copy)]
pub struct InflightSummary {
    pub dispatched_at: Instant,
    pub uncached_at_dispatch: u32,
    pub total_at_dispatch: u32,
}

/// One outstanding dispatch and how many of a query's blocks it has yet to
/// store, from one consistent read.
#[derive(Debug, Clone, Copy)]
pub struct DispatchOverlap {
    pub summary: InflightSummary,
    pub overlap_blocks: u32,
}

/// Full per-dispatch snapshot, with the hashes not yet stored.
#[derive(Debug, Clone)]
pub struct DispatchSnapshot {
    pub remaining: HashSet<u64>,
    pub dispatched_at: Instant,
    pub uncached_at_dispatch: u32,
    pub total_at_dispatch: u32,
}

impl DispatchSnapshot {
    /// Project to the `InflightSummary` view, without the hash set.
    pub fn summary(&self) -> InflightSummary {
        InflightSummary {
            dispatched_at: self.dispatched_at,
            uncached_at_dispatch: self.uncached_at_dispatch,
            total_at_dispatch: self.total_at_dispatch,
        }
    }
}

/// Per-endpoint tracker of dispatched prompt prefixes whose `BlockStored`
/// events haven't landed yet, so the picker can credit a worker for blocks
/// about to be in its KV cache.
///
/// The radix tree only reflects emitted `BlockStored` events, so it
/// under-reports overlap while a prior turn is still queued or prefilling.
/// See `picker::compute_layered_overlap`.
///
/// Same lifecycle and endpoint key (`score_addr`) as `InflightRegistry`,
/// which is just a count.
#[derive(Default)]
pub struct InflightPrefixes {
    /// Keyed by `request_id` so `forget(id)` is O(1). Per-endpoint order is
    /// recovered from `dispatched_at` at query time.
    dispatches: DashMap<u64, InflightDispatch>,
}

impl InflightPrefixes {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every outstanding dispatch on `endpoint`, oldest first, with the hashes
    /// it has yet to store. For tests and diagnostics; the pick path reads
    /// [`Self::overlaps_for`].
    pub fn dispatches_for(&self, endpoint: &str) -> Vec<DispatchSnapshot> {
        let mut matching: Vec<DispatchSnapshot> = self
            .dispatches
            .iter()
            .filter(|e| e.value().endpoint == endpoint)
            .map(|e| {
                let d = e.value();
                DispatchSnapshot {
                    remaining: (0..d.hashes.len())
                        .filter(|i| !d.is_stored(*i))
                        .map(|i| d.hashes.0[i])
                        .collect(),
                    dispatched_at: d.dispatched_at,
                    uncached_at_dispatch: d.uncached_at_dispatch,
                    total_at_dispatch: d.total_at_dispatch,
                }
            })
            .collect();
        matching.sort_by_key(|d| d.dispatched_at);
        matching
    }

    /// Every outstanding dispatch on `endpoint`, oldest first, without hashes.
    pub fn summaries_for(&self, endpoint: &str) -> Vec<InflightSummary> {
        let mut matching: Vec<InflightSummary> = self
            .dispatches
            .iter()
            .filter(|e| e.value().endpoint == endpoint)
            .map(|e| e.value().summary())
            .collect();
        matching.sort_by_key(|d| d.dispatched_at);
        matching
    }

    /// Every outstanding dispatch on `endpoint`, oldest first, with the count
    /// of `query`'s blocks that it has yet to store.
    pub fn overlaps_for(&self, endpoint: &str, query: &BlockQuery) -> Vec<DispatchOverlap> {
        let mut matching: Vec<DispatchOverlap> = self
            .dispatches
            .iter()
            .filter(|e| e.value().endpoint == endpoint)
            .map(|e| DispatchOverlap {
                summary: e.value().summary(),
                overlap_blocks: e.value().overlap(query),
            })
            .collect();
        matching.sort_by_key(|d| d.summary.dispatched_at);
        matching
    }

    /// Record a dispatch. Called inside the `pick_lock` critical section so
    /// the next concurrent pick sees it.
    pub fn insert(
        &self,
        request_id: u64,
        endpoint: String,
        hashes: BlockHashes,
        uncached_at_dispatch: u32,
        total_at_dispatch: u32,
    ) {
        self.dispatches.insert(
            request_id,
            InflightDispatch {
                endpoint,
                stored: vec![0; hashes.len().div_ceil(64)],
                remaining: hashes.len(),
                hashes,
                dispatched_at: Instant::now(),
                uncached_at_dispatch,
                total_at_dispatch,
            },
        );
    }

    /// Mark stored hashes on every dispatch on `endpoint`, dropping drained
    /// ones.
    pub fn on_stored(&self, endpoint: &str, stored: &[u64]) {
        if stored.is_empty() {
            return;
        }
        let mut drained: Vec<u64> = Vec::new();
        for mut entry in self.dispatches.iter_mut() {
            if entry.value().endpoint != endpoint {
                continue;
            }
            let d = entry.value_mut();
            for h in stored {
                if let Ok(i) = d.hashes.0.binary_search(h)
                    && !d.is_stored(i)
                {
                    d.stored[i / 64] |= 1 << (i % 64);
                    d.remaining -= 1;
                }
            }
            if d.remaining == 0 {
                drained.push(*entry.key());
            }
        }
        // Removed after iterating.
        for id in drained {
            self.dispatches.remove(&id);
        }
    }

    /// Drop the dispatch for `request_id`. Idempotent. Called when the pick's
    /// observation is released, like `InflightRegistry::dec`.
    pub fn forget(&self, request_id: u64) {
        self.dispatches.remove(&request_id);
    }

    /// Drop dispatches older than `max_age`. Backstop against lifecycle
    /// leaks (lost `BlockStored`, stream dropped without observation).
    pub fn sweep(&self, max_age: Duration) {
        let now = Instant::now();
        let stale: Vec<u64> = self
            .dispatches
            .iter()
            .filter(|e| now.duration_since(e.value().dispatched_at) > max_age)
            .map(|e| *e.key())
            .collect();
        for id in stale {
            self.dispatches.remove(&id);
        }
    }

    /// Pending dispatch count across all endpoints. Diagnostic only.
    pub fn len(&self) -> usize {
        self.dispatches.len()
    }

    pub fn is_empty(&self) -> bool {
        self.dispatches.is_empty()
    }
}

#[cfg(test)]
mod tests;
