use super::*;

mod contention;

#[test]
fn inc_then_get_returns_one() {
    let r = InflightRegistry::new();
    r.inc("a:1");
    assert_eq!(r.get("a:1"), 1);
}

#[test]
fn three_inc_two_dec_returns_one() {
    let r = InflightRegistry::new();
    r.inc("a:1");
    r.inc("a:1");
    r.inc("a:1");
    r.dec("a:1");
    r.dec("a:1");
    assert_eq!(r.get("a:1"), 1);
}

#[test]
fn dec_at_zero_saturates() {
    let r = InflightRegistry::new();
    r.dec("a:1"); // no entry yet
    assert_eq!(r.get("a:1"), 0);
    r.inc("a:1");
    r.dec("a:1");
    r.dec("a:1"); // already at 0
    r.dec("a:1"); // still 0
    assert_eq!(r.get("a:1"), 0);
}

#[test]
fn distinct_endpoints_dont_interfere() {
    let r = InflightRegistry::new();
    r.inc("a:1");
    r.inc("b:2");
    r.inc("b:2");
    assert_eq!(r.get("a:1"), 1);
    assert_eq!(r.get("b:2"), 2);
    assert_eq!(r.get("c:3"), 0);
}

#[test]
fn get_on_missing_returns_zero() {
    let r = InflightRegistry::new();
    assert_eq!(r.get("never-seen"), 0);
}

#[tokio::test]
async fn concurrent_inc_dec_balances() {
    let r = std::sync::Arc::new(InflightRegistry::new());
    let mut handles = vec![];
    // 16 tasks each do 1000 inc + 1000 dec on the same endpoint.
    // Net delta == 0, final count must be 0.
    for _ in 0..16 {
        let r = r.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..1000 {
                r.inc("hot:1");
            }
            for _ in 0..1000 {
                r.dec("hot:1");
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(r.get("hot:1"), 0);
}

fn hs(items: &[u64]) -> HashSet<u64> {
    items.iter().copied().collect()
}

fn bh(items: &[u64]) -> BlockHashes {
    items.iter().copied().collect()
}

fn bq(items: &[u64]) -> BlockQuery {
    items.iter().copied().collect()
}

#[test]
fn position_matches_binary_search_for_uniform_and_clustered_hashes() {
    let uniform = |i: u64| i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let low = |i: u64| i * 3;
    let high = |i: u64| u64::MAX - i * 3;
    let random = |i: u64| {
        let mut x = i.wrapping_add(0x9E37_79B9_7F4A_7C15);
        x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
        x ^ (x >> 31)
    };
    let hashes: [&dyn Fn(u64) -> u64; 4] = [&uniform, &low, &high, &random];
    for len in [
        0, 1, 2, 15, 16, 17, 31, 32, 33, 63, 64, 65, 255, 256, 257, 511, 512, 513, 1000, 2047,
        2048, 2049, 4095, 4096, 4097, 16383, 16384, 16385, 32767, 32768, 32769, 60000, 60001,
    ] {
        for hash in hashes {
            let run: BlockHashes = (0..len).map(hash).collect();
            let probes = (0..2 * len).map(hash).chain([0, 1, u64::MAX - 1, u64::MAX]);
            for probe in probes {
                assert_eq!(
                    run.position(&BloomProbe::new(probe)),
                    run.0.hashes.binary_search(&probe).ok(),
                    "{len} hashes, probe {probe}"
                );
            }
        }
    }
}

#[test]
fn bloom_filters_keep_members_and_reject_most_absent_hashes() {
    let spread = |i: u64| i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    for len in [0, 1, 64, 65, 256, 4096] {
        let hashes: Vec<_> = (0..len).map(spread).collect();
        let filter = BloomFilter::new(&hashes);
        for &hash in &hashes {
            assert!(filter.may_contain(&BloomProbe::new(hash)));
        }
        if len <= BLOOM_SKIP_THRESHOLD as u64 {
            assert!(filter.0.is_empty());
            continue;
        }
        let positives = (len..len + 20_000)
            .filter(|&i| filter.may_contain(&BloomProbe::new(spread(i))))
            .count();
        assert!(
            positives < 1000,
            "{len} hashes, {positives} false positives"
        );
    }
}

#[test]
fn insert_then_dispatches_for_returns_one_entry() {
    let p = InflightPrefixes::new();
    p.insert(1, "a:1".into(), bh(&[10, 11, 12]), 7, 100);
    let ds = p.dispatches_for("a:1");
    assert_eq!(ds.len(), 1);
    assert_eq!(ds[0].remaining, hs(&[10, 11, 12]));
    assert_eq!(ds[0].uncached_at_dispatch, 7);
    assert!(p.dispatches_for("b:2").is_empty());
}

#[test]
fn dispatches_for_returns_oldest_first_with_full_snapshot() {
    let p = InflightPrefixes::new();
    p.insert(1, "a:1".into(), bh(&[1]), 100, 500);
    // Force a tiny ordering gap so dispatched_at differs.
    std::thread::sleep(Duration::from_millis(2));
    p.insert(2, "a:1".into(), bh(&[2]), 200, 800);
    std::thread::sleep(Duration::from_millis(2));
    p.insert(3, "a:1".into(), bh(&[3]), 50, 200);
    let ds = p.dispatches_for("a:1");
    assert_eq!(ds.len(), 3);
    assert_eq!(ds[0].remaining, hs(&[1]), "oldest first: {ds:?}");
    assert_eq!(ds[1].remaining, hs(&[2]));
    assert_eq!(ds[2].remaining, hs(&[3]));
    // uncached_at_dispatch travels along with each entry.
    let blocks: Vec<u32> = ds.iter().map(|d| d.uncached_at_dispatch).collect();
    assert_eq!(blocks, vec![100, 200, 50]);
    let totals: Vec<u32> = ds.iter().map(|d| d.total_at_dispatch).collect();
    assert_eq!(totals, vec![500, 800, 200]);
    // dispatched_at is monotonic non-decreasing; callers rely on
    // position-as-age across the slice.
    assert!(ds[0].dispatched_at <= ds[1].dispatched_at);
    assert!(ds[1].dispatched_at <= ds[2].dispatched_at);
    let summaries = p.summaries_for("a:1");
    assert_eq!(summaries.len(), ds.len());
    let s1 = summaries[1];
    assert_eq!(s1.uncached_at_dispatch, 200);
    assert_eq!(s1.total_at_dispatch, 800);
    assert_eq!(s1.dispatched_at, ds[1].dispatched_at);
}

#[test]
fn on_stored_removes_hashes_and_drops_emptied_dispatches() {
    let p = InflightPrefixes::new();
    p.insert(1, "a:1".into(), bh(&[10, 11, 12]), 0, 0);
    p.insert(2, "a:1".into(), bh(&[20, 21]), 0, 0);
    p.on_stored("a:1", &[10, 21]);
    let ds = p.dispatches_for("a:1");
    assert_eq!(ds.len(), 2, "both dispatches still have hashes left");
    // Now drain dispatch #2 entirely.
    p.on_stored("a:1", &[20]);
    let ds = p.dispatches_for("a:1");
    assert_eq!(ds.len(), 1, "drained dispatch dropped");
    assert_eq!(ds[0].remaining, hs(&[11, 12]));
}

#[test]
fn on_stored_for_different_endpoint_is_a_noop() {
    let p = InflightPrefixes::new();
    p.insert(1, "a:1".into(), bh(&[10, 11]), 0, 0);
    p.on_stored("b:2", &[10, 11]); // wrong endpoint
    assert_eq!(p.dispatches_for("a:1")[0].remaining, hs(&[10, 11]));
}

#[test]
fn forget_is_idempotent() {
    let p = InflightPrefixes::new();
    p.insert(1, "a:1".into(), bh(&[10, 11]), 0, 0);
    p.forget(1);
    // Second + missing-id forgets are no-ops.
    p.forget(1);
    p.forget(999);
    assert!(p.dispatches_for("a:1").is_empty());
}

#[test]
fn sweep_drops_old_keeps_fresh() {
    let p = InflightPrefixes::new();
    p.insert(1, "a:1".into(), bh(&[1]), 0, 0);
    std::thread::sleep(Duration::from_millis(50));
    p.insert(2, "a:1".into(), bh(&[2]), 0, 0);
    // Anything older than 25ms goes: dispatch #1.
    p.sweep(Duration::from_millis(25));
    let ds = p.dispatches_for("a:1");
    assert_eq!(ds.len(), 1);
    assert_eq!(ds[0].remaining, hs(&[2]));
}

#[tokio::test]
async fn concurrent_insert_forget_balances() {
    use std::sync::Arc;
    let p = Arc::new(InflightPrefixes::new());
    let mut handles = vec![];
    // 16 tasks each insert 100 dispatches and immediately forget
    // them. Net count must be 0 at the end regardless of
    // interleaving.
    for t in 0..16_u64 {
        let p = p.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..100_u64 {
                let req_id = t * 1000 + i;
                p.insert(req_id, "hot:1".into(), bh(&[i]), 0, 0);
                p.forget(req_id);
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(p.len(), 0);
}

#[test]
fn overlaps_for_counts_blocks_not_yet_stored_oldest_first() {
    let p = InflightPrefixes::new();
    p.insert(1, "a:1".into(), bh(&[4, 1, 3, 2]), 4, 4);
    std::thread::sleep(Duration::from_millis(2));
    p.insert(2, "a:1".into(), bh(&[3, 4, 5]), 3, 3);
    p.insert(3, "b:2".into(), bh(&[1, 2]), 2, 2);
    p.on_stored("a:1", &[4]);
    let os = p.overlaps_for("a:1", &bq(&[9, 2, 3, 4]));
    let counts: Vec<u32> = os.iter().map(|o| o.overlap_blocks).collect();
    assert_eq!(counts, vec![2, 1], "4 has landed, 9 was never dispatched");
    assert_eq!(os[0].summary.total_at_dispatch, 4);
    assert!(p.overlaps_for("c:3", &bq(&[1])).is_empty());
}

#[test]
fn a_repeated_stored_hash_counts_once() {
    let p = InflightPrefixes::new();
    p.insert(1, "a:1".into(), bh(&[1, 2]), 0, 0);
    p.on_stored("a:1", &[1, 1]);
    p.on_stored("a:1", &[1]);
    assert_eq!(p.dispatches_for("a:1")[0].remaining, hs(&[2]));
    p.on_stored("a:1", &[2]);
    assert!(p.is_empty());
}

#[test]
fn a_snapshot_shares_hashes_and_preserves_progress_after_the_dispatch_drains() {
    let p = InflightPrefixes::new();
    p.insert(1, "a:1".into(), (0..130).collect(), 120, 130);
    p.on_stored("a:1", &[0, 63, 64, 129]);
    let snapshots = p.snapshots_for("a:1");
    let (summary, blocks) = &snapshots[0];
    {
        let dispatch = p.dispatches.get(&1).unwrap();
        assert!(Arc::ptr_eq(&blocks.hashes.0, &dispatch.blocks.hashes.0));
        assert_eq!(summary.dispatched_at, dispatch.dispatched_at);
    }
    assert_eq!(summary.uncached_at_dispatch, 120);
    assert_eq!(summary.total_at_dispatch, 130);

    p.on_stored("a:1", &(0..130).collect::<Vec<_>>());
    assert!(p.is_empty());
    for query in [bq(&[0, 1]), (0..130).collect(), (0..2000).collect()] {
        let expected = query
            .set
            .iter()
            .filter(|&&h| h < 130 && ![0, 63, 64, 129].contains(&h))
            .count();
        assert_eq!(blocks.overlap(&query), expected as u32);
    }
}

#[test]
fn sparse_overlaps_match_sets_across_storage_and_bitmap_boundaries() {
    for len in [1, 63, 64, 65, 127, 128, 129, 256, 4096] {
        let mine: Vec<u64> = (0..len)
            .map(|i| (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))
            .collect();
        for percent in [0, 50, 74, 75, 76, 90, 99, 100] {
            let p = InflightPrefixes::new();
            p.insert(1, "a:1".into(), mine.iter().copied().collect(), 0, 0);
            let stored_count = len * percent / 100;
            let stored = &mine[..stored_count];
            p.on_stored("a:1", stored);
            p.on_stored("a:1", stored);
            let remaining: HashSet<_> = mine[stored_count..].iter().copied().collect();
            for query_len in [0, 1, 16, 256, 4096] {
                let query: BlockQuery = (0..query_len)
                    .map(|i| (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))
                    .collect();
                let expected = remaining.intersection(&query.set).count() as u32;
                let overlaps = p.overlaps_for("a:1", &query);
                let actual = overlaps.first().map_or(0, |o| o.overlap_blocks);
                assert_eq!(
                    actual, expected,
                    "{len} hashes, {percent}% stored, {query_len} query"
                );
            }
        }
    }
}

#[test]
fn dense_walks_match_a_set_intersection() {
    for (d, q) in [
        (4, 4000),
        (4000, 4),
        (300, 300),
        (200, 3000),
        (3000, 200),
        (100, 799),
        (100, 800),
        (399, 100),
        (400, 100),
        (0, 0),
        (0, 100),
        (100, 0),
    ] {
        let p = InflightPrefixes::new();
        let mine: Vec<u64> = (0..d).map(|i| i * 3).collect();
        p.insert(1, "a:1".into(), mine.iter().copied().collect(), 0, 0);
        let stored: Vec<u64> = mine.iter().copied().filter(|h| h % 7 == 0).collect();
        p.on_stored("a:1", &stored);
        let query: Vec<u64> = (0..q).map(|i| i * 2).collect();
        let expected = p.dispatches_for("a:1")[0]
            .remaining
            .intersection(&query.iter().copied().collect())
            .count() as u32;
        let got = p.overlaps_for("a:1", &query.into_iter().collect())[0].overlap_blocks;
        assert_eq!(got, expected, "dispatch {d} blocks, query {q} blocks");
    }
}

#[test]
fn a_scan_that_jumps_across_uniform_hashes_still_counts_every_match() {
    // Spread over u64 like real block hashes, so the scan jumps to its guess.
    let spread = |i: u64| i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let p = InflightPrefixes::new();
    let mine: Vec<u64> = (0..20_000).map(spread).collect();
    p.insert(1, "a:1".into(), mine.iter().copied().collect(), 0, 0);
    p.on_stored("a:1", &mine[..500]);
    let query: Vec<u64> = (0..40).map(|i| spread(i * 450)).chain([1, 2, 3]).collect();
    let expected = p.dispatches_for("a:1")[0]
        .remaining
        .intersection(&query.iter().copied().collect())
        .count() as u32;
    assert!(
        expected > 30,
        "most of the query is dispatched and unstored"
    );
    let got = p.overlaps_for("a:1", &query.into_iter().collect())[0].overlap_blocks;
    assert_eq!(got, expected);
}
