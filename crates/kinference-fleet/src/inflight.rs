//! In-flight state: work dispatched to a worker and not yet observed.
//!
//! Two per-endpoint trackers with the same lifecycle and endpoint key
//! (`score_addr`): [`InflightRegistry`] counts picks not yet observed as
//! first-token; [`InflightPrefixes`] remembers the prompt prefixes whose
//! `BlockStored` events have not landed yet.

use std::collections::HashSet;
use std::sync::Arc;
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
pub struct BlockHashes(Arc<HashData>);

/// Small runs bypass Bloom filtering to avoid its lookup overhead.
const BLOOM_SKIP_THRESHOLD: usize = 64;
/// Start with this many adjacent hashes before doubling the search window.
const POSITION_STEP: usize = 16;
const BLOOM_KEYS_PER_BLOCK: usize = 25;
// Split-block masks: https://parquet.apache.org/docs/file-format/bloomfilter/
const BLOOM_SALTS: [u32; 8] = [
    0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31,
];

#[repr(align(32))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct BloomBlock([u32; 8]);

struct BloomProbe {
    hash: u64,
    selector: u32,
    masks: [u32; 8],
}

impl BloomProbe {
    fn new(hash: u64) -> Self {
        let mut mixed = (hash ^ (hash >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d049bb133111eb);
        mixed ^= mixed >> 31;
        Self {
            hash,
            selector: (mixed >> 32) as u32,
            masks: BLOOM_SALTS.map(|salt| 1 << ((mixed as u32).wrapping_mul(salt) >> 27)),
        }
    }

    fn block(&self, count: usize) -> usize {
        ((self.selector as u128 * count as u128) >> 32) as usize
    }
}

/// Immutable membership filter. Stored events never clear its bits.
#[derive(Debug, Default, PartialEq, Eq)]
struct BloomFilter(Box<[BloomBlock]>);

impl BloomFilter {
    fn new(hashes: &[u64]) -> Self {
        if hashes.len() <= BLOOM_SKIP_THRESHOLD {
            return Self::default();
        }
        let mut blocks = vec![BloomBlock([0; 8]); hashes.len().div_ceil(BLOOM_KEYS_PER_BLOCK)];
        for &hash in hashes {
            let probe = BloomProbe::new(hash);
            let block = probe.block(blocks.len());
            for (word, mask) in blocks[block].0.iter_mut().zip(&probe.masks) {
                *word |= mask;
            }
        }
        Self(blocks.into_boxed_slice())
    }

    fn may_contain(&self, probe: &BloomProbe) -> bool {
        if self.0.is_empty() {
            return true;
        }
        self.0[probe.block(self.0.len())]
            .0
            .iter()
            .zip(&probe.masks)
            .fold(0, |missing, (word, mask)| missing | (!word & mask))
            == 0
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct HashData {
    hashes: Box<[u64]>,
    bloom: BloomFilter,
}

impl BlockHashes {
    pub fn new(mut hashes: Vec<u64>) -> Self {
        hashes.sort_unstable();
        hashes.dedup();
        Self(Arc::new(HashData {
            bloom: BloomFilter::new(&hashes),
            hashes: hashes.into_boxed_slice(),
        }))
    }

    pub fn len(&self) -> usize {
        self.0.hashes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.hashes.is_empty()
    }

    /// The sorted position of a hash that passes the membership filter.
    /// Uniform hashes make `hash * len / 2^64` a useful starting guess.
    /// Widening preserves exact lookup for clustered hashes.
    fn position(&self, probe: &BloomProbe) -> Option<usize> {
        if !self.0.bloom.may_contain(probe) {
            return None;
        }
        let hash = probe.hash;
        let a = &self.0.hashes;
        let guess = ((hash as u128 * a.len() as u128) >> 64) as usize;
        let (mut lo, mut hi) = (guess, guess);
        let mut step = POSITION_STEP;
        if a.get(guess).is_some_and(|&x| x < hash) {
            lo = guess + 1;
            while hi < a.len() && a[hi] < hash {
                lo = hi + 1;
                hi = (hi + step).min(a.len());
                step *= 2;
            }
            hi = (hi + 1).min(a.len());
        } else {
            hi = (guess + 1).min(a.len());
            while lo > 0 && a[lo] > hash {
                hi = lo;
                lo = lo.saturating_sub(step);
                step *= 2;
            }
        }
        a[lo..hi].binary_search(&hash).ok().map(|i| lo + i)
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
        let set = hashes.0.hashes.iter().copied().collect();
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

/// [`DispatchBlocks::overlap`] looks the dispatch's hashes up in the query's
/// set when the query is at least this many times longer.
const LOOKUP_RATIO: usize = 8;
/// [`DispatchBlocks::overlap`] scans the dispatch for each query hash when
/// the dispatch is at least this many times longer.
const SEARCH_RATIO: usize = 4;
/// Walk unset bits when the remaining hashes fit in this fraction of the shorter run.
const SPARSE_RATIO: usize = 4;
/// A scan jumps ahead to its guess only when the guess is this far ahead.
const JUMP_GAP: usize = 64;
/// Entries a scan steps over, and then counts, at a time.
const SCAN_BLOCK: usize = 16;

/// One outstanding dispatch. Each set bit in `blocks.stored` marks a hash
/// whose `BlockStored` event has arrived. `blocks.remaining` counts the unset bits.
///
/// `uncached_at_dispatch` (blocks the engine had to prefill) is stored since
/// the radix tree will have moved on by the time it's needed.
/// `total_at_dispatch` is the full prompt's block count, for the quadratic
/// prefill cost model.
#[derive(Debug, Clone)]
struct InflightDispatch {
    endpoint: String,
    blocks: DispatchBlocks,
    dispatched_at: Instant,
    uncached_at_dispatch: u32,
    total_at_dispatch: u32,
}

impl InflightDispatch {
    fn summary(&self) -> InflightSummary {
        InflightSummary {
            dispatched_at: self.dispatched_at,
            uncached_at_dispatch: self.uncached_at_dispatch,
            total_at_dispatch: self.total_at_dispatch,
        }
    }
}

#[derive(Debug, Clone)]
struct DispatchBlocks {
    hashes: BlockHashes,
    stored: Vec<u64>,
    remaining: usize,
}

impl DispatchBlocks {
    fn is_stored(&self, i: usize) -> bool {
        (self.stored[i / 64] >> (i % 64)) & 1 == 1
    }

    /// How many of `query`'s blocks this dispatch has yet to store.
    ///
    /// The cheapest walk depends on how the two lengths compare: a hash lookup
    /// per dispatch block when the dispatch is much shorter, a forward scan
    /// per query block when the query is much shorter, and a merge of the two
    /// sorted runs otherwise. The ratios come from benchmarks with random
    /// 64-bit hashes. A sparse bitmap limits lookups to hashes not yet stored.
    fn overlap(&self, query: &BlockQuery) -> u32 {
        let (d, q) = (self.hashes.len(), query.hashes.len());
        if self.remaining == 0 || q == 0 {
            return 0;
        }
        if self.remaining.saturating_mul(SPARSE_RATIO) <= d.min(q) {
            self.overlap_by_remaining(&query.set)
        } else if d.saturating_mul(LOOKUP_RATIO) <= q {
            self.overlap_by_lookup(&query.set)
        } else if q.saturating_mul(SEARCH_RATIO) <= d {
            self.overlap_by_search(&query.hashes.0.hashes)
        } else {
            self.overlap_by_merge(&query.hashes.0.hashes)
        }
    }

    fn overlap_by_remaining(&self, query: &HashSet<u64>) -> u32 {
        let mut n = 0;
        for (hashes, &stored) in self.hashes.0.hashes.chunks(64).zip(&self.stored) {
            let mut pending = !stored & (u64::MAX >> (64 - hashes.len()));
            while pending != 0 {
                n += query.contains(&hashes[pending.trailing_zeros() as usize]) as u32;
                pending &= pending - 1;
            }
        }
        n
    }

    fn overlap_by_lookup(&self, query: &HashSet<u64>) -> u32 {
        let mut n = 0u32;
        for (i, h) in self.hashes.0.hashes.iter().enumerate() {
            n += (query.contains(h) && !self.is_stored(i)) as u32;
        }
        n
    }

    /// Each search moves forward from where the last one ended, one block of
    /// `SCAN_BLOCK` entries at a time, so it reads the run in order. Within the
    /// last block it counts the smaller entries without branching, which the
    /// compiler vectorizes. Across a wide gap it first jumps to
    /// `y * len / 2^64`, which is close to `y`'s place because block hashes
    /// are uniform over `u64`. The jump never lands behind the last match, so
    /// clustered hashes only fall back to the scan.
    fn overlap_by_search(&self, query: &[u64]) -> u32 {
        let a = &self.hashes.0.hashes;
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
            lo += a[lo..end].iter().map(|&x| (x < y) as usize).sum::<usize>();
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
        let a = &self.hashes.0.hashes;
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
#[cfg(any(test, feature = "test-util"))]
#[derive(Debug, Clone)]
pub struct DispatchSnapshot {
    pub remaining: HashSet<u64>,
    pub dispatched_at: Instant,
    pub uncached_at_dispatch: u32,
    pub total_at_dispatch: u32,
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
    /// it has yet to store. Available to tests through `test-util`.
    #[cfg(any(test, feature = "test-util"))]
    pub fn dispatches_for(&self, endpoint: &str) -> Vec<DispatchSnapshot> {
        let mut matching: Vec<DispatchSnapshot> = self
            .dispatches
            .iter()
            .filter(|e| e.value().endpoint == endpoint)
            .map(|e| {
                let d = e.value();
                DispatchSnapshot {
                    remaining: (0..d.blocks.hashes.len())
                        .filter(|i| !d.blocks.is_stored(*i))
                        .map(|i| d.blocks.hashes.0.hashes[i])
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
        // Collect before overlap so the scan holds no shard read lock.
        let snapshots = self.snapshots_for(endpoint);
        let mut matching: Vec<DispatchOverlap> = snapshots
            .into_iter()
            .map(|(summary, blocks)| DispatchOverlap {
                summary,
                overlap_blocks: blocks.overlap(query),
            })
            .collect();
        matching.sort_by_key(|d| d.summary.dispatched_at);
        matching
    }

    fn snapshots_for(&self, endpoint: &str) -> Vec<(InflightSummary, DispatchBlocks)> {
        self.dispatches
            .iter()
            .filter(|e| e.value().endpoint == endpoint)
            .map(|e| (e.value().summary(), e.value().blocks.clone()))
            .collect()
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
                blocks: DispatchBlocks {
                    remaining: hashes.len(),
                    stored: vec![0; hashes.len().div_ceil(64)],
                    hashes,
                },
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
        let probes: Vec<_> = stored.iter().copied().map(BloomProbe::new).collect();
        let mut drained: Vec<u64> = Vec::new();
        for mut entry in self.dispatches.iter_mut() {
            if entry.value().endpoint != endpoint {
                continue;
            }
            let d = entry.value_mut();
            for probe in &probes {
                if let Some(i) = d.blocks.hashes.position(probe)
                    && !d.blocks.is_stored(i)
                {
                    d.blocks.stored[i / 64] |= 1 << (i % 64);
                    d.blocks.remaining -= 1;
                }
            }
            if d.blocks.remaining == 0 {
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
