use super::*;
use std::hint::black_box;
use std::io::Write;
use std::sync::{Barrier, Mutex};

const WORKERS: usize = 32;
const DISPATCHES: usize = 512;
const EVENT_BLOCKS: usize = 16;
const RUN_TIME: Duration = Duration::from_secs(2);

struct RunCase {
    blocks: usize,
    query_blocks: usize,
    readers: usize,
    events: bool,
    mode: Mode,
    repeat: usize,
    duration: Duration,
    record_samples: bool,
}

macro_rules! isolated_cases {
    ($(($main:ident, $current:ident, $blocks:expr, $seconds:expr)),* $(,)?) => {
        $(
            #[test]
            #[ignore = "benchmark worker: run through tests/bench_inflight.py"]
            fn $main() {
                isolated_case(Mode::Hash, $blocks, $seconds);
            }

            #[test]
            #[ignore = "benchmark worker: run through tests/bench_inflight.py"]
            fn $current() {
                isolated_case(Mode::Owned, $blocks, $seconds);
            }
        )*
    };
}

isolated_cases!(
    (isolated_main_256, isolated_current_256, 256, 5),
    (isolated_main_4096, isolated_current_4096, 4096, 5),
    (isolated_main_10000, isolated_current_10000, 10_000, 5),
    (isolated_main_20000, isolated_current_20000, 20_000, 10),
    (isolated_main_30000, isolated_current_30000, 30_000, 15),
    (isolated_main_40000, isolated_current_40000, 40_000, 20),
    (isolated_main_50000, isolated_current_50000, 50_000, 25),
    (isolated_main_60000, isolated_current_60000, 60_000, 30),
);

fn isolated_case(mode: Mode, blocks: usize, seconds: u64) {
    run_case(RunCase {
        blocks,
        query_blocks: blocks,
        readers: 4,
        events: true,
        mode,
        repeat: 0,
        duration: Duration::from_secs(seconds),
        record_samples: true,
    });
}

fn phase(name: &str) {
    println!("BENCH_PHASE {name}");
    std::io::stdout().flush().unwrap();
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    Hash,
    Owned,
}

enum Tracker {
    Hash(DashMap<u64, (String, DispatchSnapshot)>),
    Sorted(InflightPrefixes),
}

impl Tracker {
    fn new(mode: Mode, endpoints: &[String], hashes: &[u64]) -> Self {
        Self::with_prompts(mode, endpoints, |_| hashes.to_vec())
    }

    /// Dispatch `id` goes to endpoint `id % WORKERS` with `prompt(id)`.
    fn with_prompts(mode: Mode, endpoints: &[String], prompt: impl Fn(usize) -> Vec<u64>) -> Self {
        let tracker = match mode {
            Mode::Hash => Self::Hash(DashMap::new()),
            Mode::Owned => Self::Sorted(InflightPrefixes::new()),
        };
        for id in 0..DISPATCHES {
            let endpoint = endpoints[id % WORKERS].clone();
            let hashes = prompt(id);
            match &tracker {
                Self::Hash(dispatches) => {
                    dispatches.insert(
                        id as u64,
                        (
                            endpoint,
                            DispatchSnapshot {
                                remaining: hashes.iter().copied().collect(),
                                dispatched_at: Instant::now(),
                                uncached_at_dispatch: hashes.len() as u32,
                                total_at_dispatch: hashes.len() as u32,
                            },
                        ),
                    );
                }
                Self::Sorted(prefixes) => prefixes.insert(
                    id as u64,
                    endpoint,
                    BlockHashes::new(hashes.clone()),
                    hashes.len() as u32,
                    hashes.len() as u32,
                ),
            }
        }
        tracker
    }

    fn on_stored(&self, endpoint: &str, stored: &[u64]) {
        match self {
            Self::Hash(dispatches) => {
                let mut drained = Vec::new();
                for mut entry in dispatches.iter_mut() {
                    if entry.value().0 != endpoint {
                        continue;
                    }
                    for hash in stored {
                        entry.value_mut().1.remaining.remove(hash);
                    }
                    if entry.value().1.remaining.is_empty() {
                        drained.push(*entry.key());
                    }
                }
                for id in drained {
                    dispatches.remove(&id);
                }
            }
            Self::Sorted(prefixes) => prefixes.on_stored(endpoint, stored),
        }
    }

    fn read(&self, endpoints: &[String], hashes: &[u64], query: &BlockQuery) -> u64 {
        match self {
            Self::Hash(dispatches) => {
                let snapshots: Vec<Vec<_>> = endpoints
                    .iter()
                    .map(|endpoint| {
                        let mut snapshots: Vec<_> = dispatches
                            .iter()
                            .filter(|entry| entry.value().0 == *endpoint)
                            .map(|entry| entry.value().1.clone())
                            .collect();
                        snapshots.sort_by_key(|dispatch| dispatch.dispatched_at);
                        snapshots
                    })
                    .collect();
                let mut count = 0;
                for endpoint_dispatches in &snapshots {
                    let query: HashSet<_> = hashes.iter().copied().collect();
                    for dispatch in endpoint_dispatches {
                        count += dispatch.remaining.intersection(&query).count() as u64;
                    }
                }
                black_box(snapshots);
                count
            }
            Self::Sorted(prefixes) => {
                let overlaps: Vec<_> = endpoints
                    .iter()
                    .map(|endpoint| prefixes.overlaps_for(endpoint, query))
                    .collect();
                let count = overlaps
                    .iter()
                    .flatten()
                    .map(|o| o.overlap_blocks as u64)
                    .sum();
                black_box(overlaps);
                count
            }
        }
    }
}

fn describe(samples: &mut [Duration]) -> serde_json::Value {
    if samples.is_empty() {
        return serde_json::json!({"n": 0});
    }
    samples.sort_unstable();
    let us = |d: Duration| d.as_secs_f64() * 1e6;
    serde_json::json!({
        "n": samples.len(),
        "mean_us": samples.iter().map(|d| us(*d)).sum::<f64>() / samples.len() as f64,
        "p50_us": us(samples[(samples.len() - 1) / 2]),
        "p99_us": us(samples[(samples.len() - 1) * 99 / 100]),
        "max_us": us(*samples.last().unwrap()),
    })
}

/// Deterministic SplitMix64 output for benchmark keys.
fn fixture_hash(i: u64) -> u64 {
    let mut x = i.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

fn fixture(dispatch_blocks: usize, query_blocks: usize) -> (Vec<String>, Vec<u64>, Vec<u64>) {
    let hashes = (0..dispatch_blocks as u64).map(fixture_hash).collect();
    let shared = dispatch_blocks.min(query_blocks) / 2;
    let query = (0..query_blocks)
        .map(|i| {
            fixture_hash(if i < shared {
                (i * dispatch_blocks / shared) as u64
            } else {
                (i + dispatch_blocks) as u64
            })
        })
        .collect();
    let endpoints = (0..WORKERS).map(|i| format!("worker-{i}:8000")).collect();
    (endpoints, hashes, query)
}

#[test]
fn benchmark_fixture_preserves_lengths_and_overlap() {
    for (dispatch, query, shared) in [(256, 4096, 128), (4096, 256, 128), (4096, 4096, 2048)] {
        let (_, hashes, query_hashes) = fixture(dispatch, query);
        let hashes: HashSet<_> = hashes.into_iter().collect();
        let query_hashes: HashSet<_> = query_hashes.into_iter().collect();
        assert_eq!(hashes.len(), dispatch);
        assert_eq!(query_hashes.len(), query);
        assert_eq!(hashes.intersection(&query_hashes).count(), shared);
    }
}

fn walk(dispatch_blocks: usize, query_blocks: usize) -> &'static str {
    if dispatch_blocks * LOOKUP_RATIO <= query_blocks {
        "lookup"
    } else if query_blocks * SEARCH_RATIO <= dispatch_blocks {
        "search"
    } else {
        "merge"
    }
}

#[test]
#[ignore = "benchmark: run with --release --ignored --nocapture"]
fn picks_with_stored_events() {
    run_picks(&[(256, 256), (4096, 4096)]);
}

#[test]
#[ignore = "benchmark: run with --release --ignored --nocapture"]
fn lookup_with_stored_events() {
    run_picks(&[(256, 4096)]);
}

#[test]
#[ignore = "benchmark: run with --release --ignored --nocapture"]
fn search_with_stored_events() {
    run_picks(&[(4096, 256)]);
}

#[test]
#[ignore = "benchmark: run with --release --ignored --nocapture"]
fn large_picks_with_stored_events() {
    for repeat in 0..3 {
        for blocks in [10_000, 20_000] {
            let modes = [Mode::Hash, Mode::Owned];
            for i in 0..modes.len() {
                run(
                    blocks,
                    blocks,
                    4,
                    true,
                    modes[(i + repeat) % modes.len()],
                    repeat,
                );
            }
        }
    }
}

fn run_picks(cases: &[(usize, usize)]) {
    for repeat in 0..3 {
        for &(blocks, query_blocks) in cases {
            for readers in [1, 4] {
                for events in [false, true] {
                    let modes = [Mode::Hash, Mode::Owned];
                    for i in 0..modes.len() {
                        run(
                            blocks,
                            query_blocks,
                            readers,
                            events,
                            modes[(i + repeat) % modes.len()],
                            repeat,
                        );
                    }
                }
            }
        }
    }
}

#[test]
#[ignore = "benchmark: run with --release --ignored --nocapture"]
fn query_preparation() {
    prepare_cases(&[256, 4096], 10_000);
}

#[test]
#[ignore = "benchmark: run with --release --ignored --nocapture"]
fn large_query_preparation() {
    prepare_cases(&[10_000, 20_000], 1_000);
}

fn prepare_cases(block_counts: &[usize], iterations: usize) {
    for repeat in 0..5 {
        for &blocks in block_counts {
            let (_, _, hashes) = fixture(blocks, blocks);
            let mut samples = Vec::new();
            for _ in 0..iterations {
                let input = hashes.clone();
                let began = Instant::now();
                let query = BlockQuery::new(BlockHashes::new(input));
                black_box(&query);
                samples.push(began.elapsed());
            }
            println!(
                "BENCH {}",
                serde_json::json!({
                    "kind": "prepare", "fixture": "splitmix64", "repeat": repeat, "blocks": blocks,
                    "mode": "Owned", "prepare": describe(&mut samples),
                })
            );
        }
    }
}

#[test]
#[ignore = "benchmark: run with --release --ignored --nocapture"]
fn bloom_filter_stats() {
    for blocks in [256, 4096, 10_000, 20_000] {
        let hashes: Vec<_> = (0..blocks).map(fixture_hash).collect();
        let filter = BloomFilter::new(&hashes);
        let samples = 100_000;
        let false_positives = (blocks..blocks + samples)
            .filter(|&i| filter.may_contain(&BloomProbe::new(fixture_hash(i))))
            .count();
        println!(
            "BENCH {}",
            serde_json::json!({
                "kind": "bloom", "fixture": "splitmix64", "blocks": blocks, "samples": samples,
                "false_positives": false_positives,
                "filter_bytes": filter.0.len() * std::mem::size_of::<BloomBlock>(),
            })
        );
    }
}

#[test]
#[ignore = "benchmark: run with --release --ignored --nocapture"]
fn stored_without_picks() {
    stored_cases(&[256, 4096]);
}

#[test]
#[ignore = "benchmark: run with --release --ignored --nocapture"]
fn large_stored_without_picks() {
    stored_cases(&[10_000, 20_000]);
}

fn stored_cases(block_counts: &[usize]) {
    for repeat in 0..5 {
        for &blocks in block_counts {
            for shared in [blocks, blocks / 2, 0] {
                let modes = [Mode::Hash, Mode::Owned];
                for i in 0..modes.len() {
                    let mode = modes[(i + repeat) % modes.len()];
                    let mut samples = stored_alone(mode, blocks, shared);
                    println!(
                        "BENCH {}",
                        serde_json::json!({
                            "kind": "stored_alone", "fixture": "splitmix64", "repeat": repeat, "blocks": blocks,
                            "shared": shared, "mode": format!("{mode:?}"),
                            "event": describe(&mut samples),
                        })
                    );
                }
            }
        }
    }
}

/// Each dispatch shares its first `shared` hashes with every other dispatch.
/// Events carry the first half of each endpoint's oldest dispatch. A hash
/// past `shared` therefore matches that dispatch and misses the other 15.
fn stored_alone(mode: Mode, blocks: usize, shared: usize) -> Vec<Duration> {
    let prompt = |id: usize| -> Vec<u64> {
        (0..blocks)
            .map(|b| fixture_hash(if b < shared { b } else { (id + 1) * blocks + b } as u64))
            .collect()
    };
    let endpoints: Vec<String> = (0..WORKERS).map(|i| format!("worker-{i}:8000")).collect();
    let tracker = Tracker::with_prompts(mode, &endpoints, prompt);
    let oldest: Vec<Vec<u64>> = (0..WORKERS).map(prompt).collect();
    let mut samples = Vec::new();
    for offset in (0..blocks / 2).step_by(EVENT_BLOCKS) {
        for (endpoint, hashes) in endpoints.iter().zip(&oldest) {
            let began = Instant::now();
            tracker.on_stored(endpoint, &hashes[offset..offset + EVENT_BLOCKS]);
            samples.push(began.elapsed());
        }
    }
    samples
}

fn run(
    blocks: usize,
    query_blocks: usize,
    readers: usize,
    events: bool,
    mode: Mode,
    repeat: usize,
) {
    let duration = if blocks.max(query_blocks) >= 10_000 {
        Duration::from_secs(5)
    } else {
        RUN_TIME
    };
    run_case(RunCase {
        blocks,
        query_blocks,
        readers,
        events,
        mode,
        repeat,
        duration,
        record_samples: false,
    });
}

fn run_case(case: RunCase) {
    let RunCase {
        blocks,
        query_blocks,
        readers,
        events,
        mode,
        repeat,
        duration: run_time,
        record_samples,
    } = case;
    let (endpoints, hashes, query_hashes) = fixture(blocks, query_blocks);
    let tracker = Tracker::new(mode, &endpoints, &hashes);
    if record_samples {
        phase("retained");
        std::thread::sleep(Duration::from_millis(150));
    }
    let pick_lock = Mutex::new(());
    let start = Barrier::new(readers + 2);
    let event_limit = 512 * run_time.as_secs() as usize / RUN_TIME.as_secs() as usize;
    let event_budget = (WORKERS * blocks / EVENT_BLOCKS / 4).min(event_limit);
    let event_count = event_budget / WORKERS * WORKERS;
    let (samples, mut stored, elapsed, counts) = std::thread::scope(|scope| {
        let threads: Vec<_> = (0..readers)
            .map(|_| {
                scope.spawn(|| {
                    start.wait();
                    let until = Instant::now() + run_time;
                    let mut samples = Vec::new();
                    while Instant::now() < until {
                        // Scoring supplies an owned vector before commit starts.
                        let input = query_hashes.clone();
                        let began = Instant::now();
                        let query = match mode {
                            Mode::Hash => BlockQuery::default(),
                            Mode::Owned => BlockQuery::new(BlockHashes::new(input)),
                        };
                        let prepared = Instant::now();
                        let guard = pick_lock.lock().unwrap();
                        let acquired = Instant::now();
                        black_box(tracker.read(&endpoints, &query_hashes, &query));
                        let finished = Instant::now();
                        drop(guard);
                        samples.push([
                            finished - began,
                            prepared - began,
                            acquired - prepared,
                            finished - acquired,
                        ]);
                    }
                    samples
                })
            })
            .collect();
        let writer = scope.spawn(|| {
            start.wait();
            let began = Instant::now();
            let mut samples = Vec::new();
            for event in 0..if events { event_count } else { 0 } {
                let scheduled = began + run_time.mul_f64((event + 1) as f64 / event_count as f64);
                std::thread::sleep(scheduled.saturating_duration_since(Instant::now()));
                let endpoint = event % WORKERS;
                let offset = event / WORKERS * EVENT_BLOCKS;
                let began = Instant::now();
                tracker.on_stored(&endpoints[endpoint], &hashes[offset..offset + EVENT_BLOCKS]);
                samples.push(began.elapsed());
            }
            samples
        });
        if record_samples {
            phase("running");
        }
        let began = Instant::now();
        start.wait();
        let per_thread: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        let counts: Vec<_> = per_thread.iter().map(Vec::len).collect();
        let samples: Vec<_> = per_thread.into_iter().flatten().collect();
        let stored = writer.join().unwrap();
        (samples, stored, began.elapsed(), counts)
    });
    if record_samples {
        phase("finished");
    }
    let stored_blocks = if events {
        event_count / WORKERS * EVENT_BLOCKS
    } else {
        0
    };
    let expected = expected_overlap(&hashes, &query_hashes, stored_blocks);
    let query = query_hashes.iter().copied().collect();
    assert_eq!(tracker.read(&endpoints, &query_hashes, &query), expected);
    let column = |i| samples.iter().map(|s| s[i]).collect::<Vec<_>>();
    println!(
        "BENCH {}",
        serde_json::json!({
            "kind": "picks", "fixture": "splitmix64", "repeat": repeat, "mode": format!("{mode:?}"),
            "blocks": blocks, "query_blocks": query_blocks, "walk": walk(blocks, query_blocks),
            "run_seconds": run_time.as_secs(), "elapsed_seconds": elapsed.as_secs_f64(),
            "scheduled_events": if events { event_count } else { 0 },
            "readers": readers, "events": events, "expected": expected,
            "picks_s": samples.len() as f64 / elapsed.as_secs_f64(), "per_thread": counts,
            "total": describe(&mut column(0)), "prepare": describe(&mut column(1)),
            "wait": describe(&mut column(2)), "held": describe(&mut column(3)),
            "event": describe(&mut stored),
            "samples_ns": record_samples.then(|| samples.iter()
                .map(|sample| sample.map(|d| d.as_nanos() as u64)).collect::<Vec<_>>()),
        })
    );
}

fn expected_overlap(hashes: &[u64], query: &[u64], stored_blocks: usize) -> u64 {
    let remaining: HashSet<_> = hashes[stored_blocks..].iter().copied().collect();
    let query: HashSet<_> = query.iter().copied().collect();
    remaining.intersection(&query).count() as u64 * DISPATCHES as u64
}

#[test]
#[ignore = "benchmark: run with --release --ignored --nocapture"]
fn partial_storage() {
    partial_storage_at(&[0, 50, 90, 99]);
}

#[test]
#[ignore = "benchmark: run with --release --ignored --nocapture"]
fn partial_storage_threshold() {
    partial_storage_at(&[74, 75, 76]);
}

fn partial_storage_at(percentages: &[usize]) {
    partial_storage_cases(
        &[(256, 256), (4096, 4096), (256, 4096), (4096, 256)],
        percentages,
    );
}

#[test]
#[ignore = "benchmark: run with --release --ignored --nocapture"]
fn large_partial_storage() {
    partial_storage_cases(&[(10_000, 10_000), (20_000, 20_000)], &[99]);
}

fn partial_storage_cases(cases: &[(usize, usize)], percentages: &[usize]) {
    for repeat in 0..3 {
        for &(blocks, query_blocks) in cases {
            for &stored_percent in percentages {
                let modes = [Mode::Hash, Mode::Owned];
                for i in 0..modes.len() {
                    let mode = modes[(i + repeat) % modes.len()];
                    let (endpoints, hashes, query_hashes) = fixture(blocks, query_blocks);
                    let tracker = Tracker::new(mode, &endpoints, &hashes);
                    let stored_blocks = blocks * stored_percent / 100;
                    for endpoint in &endpoints {
                        tracker.on_stored(endpoint, &hashes[..stored_blocks]);
                    }
                    let query = query_hashes.iter().copied().collect();
                    let expected = expected_overlap(&hashes, &query_hashes, stored_blocks);
                    assert_eq!(tracker.read(&endpoints, &query_hashes, &query), expected);
                    for _ in 0..3 {
                        black_box(tracker.read(&endpoints, &query_hashes, &query));
                    }
                    let mut samples = Vec::new();
                    let until = Instant::now() + Duration::from_millis(200);
                    while Instant::now() < until {
                        let began = Instant::now();
                        black_box(tracker.read(&endpoints, &query_hashes, &query));
                        samples.push(began.elapsed());
                    }
                    println!(
                        "BENCH {}",
                        serde_json::json!({
                            "kind": "partial", "fixture": "splitmix64", "repeat": repeat, "blocks": blocks,
                            "query_blocks": query_blocks,
                            "walk": if matches!(mode, Mode::Owned)
                                && (blocks - stored_blocks).saturating_mul(SPARSE_RATIO) <= blocks.min(query_blocks)
                                { "remaining" } else { walk(blocks, query_blocks) },
                            "stored_percent": stored_percent, "mode": format!("{mode:?}"),
                            "expected": expected, "read": describe(&mut samples),
                        })
                    );
                }
            }
        }
    }
}
