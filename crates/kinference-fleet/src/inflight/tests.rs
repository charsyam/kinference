use super::*;

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
    // `.summary()` projects to the cheaper InflightSummary.
    let s1 = ds[1].summary();
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
fn every_walk_matches_a_set_intersection() {
    // Lengths that send `overlap` down each of its three walks.
    for (d, q) in [(4, 4000), (4000, 4), (300, 300), (200, 3000), (3000, 200)] {
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

#[cfg(target_arch = "x86_64")]
#[test]
fn avx2_counts_unsigned_values_and_partial_blocks() {
    if !std::arch::is_x86_feature_detected!("avx2") {
        return;
    }
    let values = [
        0,
        1,
        u64::MAX,
        1 << 63,
        (1 << 63) - 1,
        u64::MAX - 1,
        17,
        17,
        42,
        0,
        1 << 63,
        100,
        200,
        300,
        400,
        500,
    ];
    for start in 0..=values.len() {
        for end in start..=values.len() {
            for y in [
                0,
                1,
                17,
                42,
                (1 << 63) - 1,
                1 << 63,
                (1 << 63) + 1,
                u64::MAX,
            ] {
                let block = &values[start..end];
                let expected = block.iter().filter(|&&x| x < y).count();
                assert_eq!(unsafe { count_less_avx2(block, y) }, expected);
            }
        }
    }
}
