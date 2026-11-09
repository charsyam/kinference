# In-flight tracker benchmarks

The baseline is the main HashSet tracker at be2a7ad. Current uses sorted
hashes, immutable Bloom filtering and interpolation. The harness
measures tracker work rather than the full server.

Counts describe block hashes, not tokens. A full block contains
kvBlockSize tokens. The fixture applies SplitMix64 to consecutive integer
keys. Every benchmark uses the same deterministic random-looking hashes.

## Method

The host is macOS on an Apple M4 Pro with 14 logical CPUs and 48 GiB
RAM. The compiler is rustc 1.95.0 for aarch64-apple-darwin. The
benchmark uses the release profile.

Each case has 32 workers, 512 outstanding dispatches and four picking
threads. All dispatches contain the same hashes. Query length equals
dispatch length. Each query matches half the dispatch hashes. Stored
events contain 16 hashes.

Each implementation and block count runs in a fresh process. Each case
repeats three times. The runner rotates implementation order. Case
windows are 5 seconds through 10,000 blocks, 10 seconds at 20,000, 15
seconds at 30,000, 20 seconds at 40,000, 25 seconds at 50,000 and 30
seconds at 60,000.

Stored events arrive at 256/s from 4,096 blocks upward. The 256-block
case spreads 128 events across five seconds. The benchmark checks final
overlap counts against a set-intersection oracle.

## Throughput

Throughput divides completed picks by actual elapsed time across all
repeats. Elapsed time includes the final drain after the case window. N
counts completed picks.

| Blocks | main picks/s | Current picks/s | Ratio | main N | Current N |
| --- | --- | --- | --- | --- | --- |
| 256 | 351.06 | 1444.58 | 4.11x | 5276 | 21690 |
| 4,096 | 22.20 | 115.39 | 5.20x | 343 | 1741 |
| 10,000 | 8.52 | 48.27 | 5.66x | 138 | 735 |
| 20,000 | 4.25 | 24.25 | 5.71x | 138 | 738 |
| 30,000 | 2.77 | 15.87 | 5.72x | 135 | 724 |
| 40,000 | 2.02 | 11.76 | 5.81x | 132 | 716 |
| 50,000 | 1.49 | 9.05 | 6.07x | 123 | 689 |
| 60,000 | 1.32 | 7.07 | 5.37x | 129 | 647 |

## Held time

Held time measures in-flight tracker work under pick_lock. Held time
excludes mutex waiting and the rest of router commit. Means pool all
completed picks across threads and repeats.

| Blocks | main held ms | Current held ms |
| --- | --- | --- |
| 256 | 2.846 | 0.685 |
| 4,096 | 45.030 | 8.656 |
| 10,000 | 117.317 | 20.705 |
| 20,000 | 235.376 | 41.215 |
| 30,000 | 360.564 | 62.993 |
| 40,000 | 493.899 | 84.984 |
| 50,000 | 669.848 | 110.422 |
| 60,000 | 759.315 | 141.507 |

## Queue-inclusive latency diagnostics

The latency timer starts before query preparation and ends after tracker
reads. The latency timer includes mutex waiting. Fixture generation and
input cloning are outside the latency timer. Percentiles use nearest
ranks across all completed picks from every thread and repeat.

Mutex admission can concentrate completed picks on one thread. A few
much longer waits can lie above p99. Maximum latency and per-thread
completion counts expose long waits. The minimum is the smallest number
of picks completed by one thread in any repeat. Queue-inclusive
percentiles do not isolate tracker cost.

| Blocks | main p50 ms | Current p50 ms | main p99 ms | Current p99 ms | main max ms | Current max ms |
| --- | --- | --- | --- | --- | --- | --- |
| 256 | 2.820 | 0.684 | 3.967 | 52.791 | 5007.908 | 640.827 |
| 4,096 | 45.067 | 34.464 | 4785.250 | 37.842 | 5079.284 | 63.624 |
| 10,000 | 117.731 | 82.299 | 5310.828 | 90.462 | 5380.299 | 145.489 |
| 20,000 | 236.952 | 163.345 | 8976.998 | 179.262 | 9418.403 | 182.643 |
| 30,000 | 1425.619 | 250.561 | 5693.360 | 276.290 | 6786.790 | 287.059 |
| 40,000 | 1943.280 | 335.573 | 4034.300 | 383.949 | 4883.237 | 395.319 |
| 50,000 | 2666.198 | 439.086 | 2811.988 | 489.473 | 4574.198 | 503.865 |
| 60,000 | 2973.859 | 565.490 | 5231.851 | 616.543 | 5363.687 | 627.744 |

| Blocks | main minimum completions per thread | Current minimum completions per thread |
| --- | --- | --- |
| 256 | 1 | 1099 |
| 4,096 | 1 | 143 |
| 10,000 | 1 | 60 |
| 20,000 | 2 | 60 |
| 30,000 | 7 | 59 |
| 40,000 | 9 | 58 |
| 50,000 | 10 | 56 |
| 60,000 | 9 | 53 |

## Resident memory

RSS is sampled every 20 ms through proc_pidinfo on macOS or proc statm
on Linux. Retained RSS is the median after tracker construction. Sampled
peak is the largest RSS sample while picks execute. Kernel peak is the
process high-water mark from wait4. Kernel peak includes validation and
output serialization after the timed workload.

RSS includes the fixture, tracker, threads, allocator state and
benchmark buffers. Fresh processes prevent allocator state from carrying
between implementations. Memory values use MiB. Retained values are
medians across repeats. Peak values are maxima across repeats.

| Blocks | main retained MiB | Current retained MiB | main sampled peak MiB | Current sampled peak MiB | main kernel peak MiB | Current kernel peak MiB |
| --- | --- | --- | --- | --- | --- | --- |
| 256 | 9.86 | 8.56 | 13.25 | 10.55 | 13.97 | 11.67 |
| 4,096 | 47.53 | 26.75 | 136.59 | 28.56 | 136.88 | 28.86 |
| 10,000 | 87.84 | 55.58 | 274.77 | 58.08 | 275.14 | 58.33 |
| 20,000 | 160.53 | 103.75 | 478.39 | 108.38 | 478.66 | 108.59 |
| 30,000 | 305.56 | 154.89 | 872.41 | 163.66 | 873.88 | 163.88 |
| 40,000 | 305.58 | 203.94 | 906.44 | 212.17 | 907.25 | 212.41 |
| 50,000 | 305.83 | 245.00 | 881.56 | 256.70 | 882.34 | 256.91 |
| 60,000 | 595.86 | 294.39 | 1787.00 | 312.02 | 1790.50 | 312.23 |

## Lookup and search paths

Lookup uses 256 dispatch hashes and 4,096 query hashes. Search reverses
the lengths. Each configuration runs three times for two seconds.
Throughput pools completed picks and actual elapsed time.

| Path | Picking threads | Stored events | main picks/s | Current picks/s | Ratio |
| --- | --- | --- | --- | --- | --- |
| lookup | 1 | no | 208.60 | 681.12 | 3.27x |
| lookup | 1 | yes | 225.98 | 686.76 | 3.04x |
| lookup | 4 | no | 203.59 | 783.71 | 3.85x |
| lookup | 4 | yes | 225.48 | 745.14 | 3.30x |
| search | 1 | no | 223.84 | 614.46 | 2.75x |
| search | 1 | yes | 221.03 | 616.02 | 2.79x |
| search | 4 | no | 230.96 | 614.96 | 2.66x |
| search | 4 | yes | 230.82 | 617.69 | 2.68x |

## Stored events without picks

Each event contains 16 hashes. Each configuration repeats five times.
Means pool timed events across repeats. Shared percentage describes
hashes that every dispatch contains. Events use the first half of the
oldest dispatch on each endpoint. The 50% case therefore tests mostly
matching hashes.

| Blocks | Shared | main event mean us | Current event mean us | Current/main time |
| --- | --- | --- | --- | --- |
| 256 | 0% | 8.09 | 6.32 | 0.781x |
| 256 | 50% | 11.11 | 8.21 | 0.739x |
| 256 | 100% | 10.90 | 7.78 | 0.714x |
| 4,096 | 0% | 9.88 | 6.88 | 0.697x |
| 4,096 | 50% | 17.81 | 14.03 | 0.788x |
| 4,096 | 100% | 17.24 | 12.35 | 0.717x |
| 10,000 | 0% | 12.28 | 7.44 | 0.606x |
| 10,000 | 50% | 21.02 | 20.25 | 0.964x |
| 10,000 | 100% | 22.06 | 16.46 | 0.746x |
| 20,000 | 0% | 13.07 | 8.51 | 0.651x |
| 20,000 | 50% | 23.65 | 25.61 | 1.083x |
| 20,000 | 100% | 24.02 | 17.24 | 0.718x |

The 20,000-block case with 50% shared hashes takes more time than main.
The event-only result includes shard iteration and removal work. The
event-only result does not measure pick throughput.

## Partially stored dispatches

Each read includes all 32 endpoints and 512 dispatches. Each
configuration repeats three times. Each repeat samples reads for 200 ms.
Means pool timed reads across repeats.

| Dispatch blocks | Query blocks | Stored | main read mean us | Current read mean us | main/current time |
| --- | --- | --- | --- | --- | --- |
| 256 | 256 | 0% | 3162.49 | 680.51 | 4.65x |
| 256 | 256 | 50% | 1798.51 | 689.09 | 2.61x |
| 256 | 256 | 74% | 1113.25 | 691.53 | 1.61x |
| 256 | 256 | 75% | 1159.96 | 453.29 | 2.56x |
| 256 | 256 | 76% | 1099.31 | 460.23 | 2.39x |
| 256 | 256 | 90% | 628.04 | 294.46 | 2.13x |
| 256 | 256 | 99% | 416.00 | 188.99 | 2.20x |
| 256 | 4,096 | 0% | 6354.88 | 1583.98 | 4.01x |
| 256 | 4,096 | 50% | 3581.27 | 1237.86 | 2.89x |
| 256 | 4,096 | 74% | 2789.19 | 1280.52 | 2.18x |
| 256 | 4,096 | 75% | 2790.52 | 471.81 | 5.91x |
| 256 | 4,096 | 76% | 3095.33 | 447.43 | 6.92x |
| 256 | 4,096 | 90% | 2232.57 | 299.41 | 7.46x |
| 256 | 4,096 | 99% | 1953.03 | 189.16 | 10.33x |
| 4,096 | 256 | 0% | 4662.16 | 1626.26 | 2.87x |
| 4,096 | 256 | 50% | 4404.29 | 1680.16 | 2.62x |
| 4,096 | 256 | 74% | 4063.37 | 1669.08 | 2.43x |
| 4,096 | 256 | 75% | 4021.37 | 1660.60 | 2.42x |
| 4,096 | 256 | 76% | 3920.27 | 1655.16 | 2.37x |
| 4,096 | 256 | 90% | 3319.52 | 1682.86 | 1.97x |
| 4,096 | 256 | 99% | 2252.65 | 486.27 | 4.63x |
| 4,096 | 4,096 | 0% | 48640.34 | 8754.44 | 5.56x |
| 4,096 | 4,096 | 50% | 27048.30 | 8707.65 | 3.11x |
| 4,096 | 4,096 | 74% | 17175.04 | 8707.65 | 1.97x |
| 4,096 | 4,096 | 75% | 16492.90 | 4850.16 | 3.40x |
| 4,096 | 4,096 | 76% | 16301.49 | 4673.07 | 3.49x |
| 4,096 | 4,096 | 90% | 9923.42 | 2114.04 | 4.69x |
| 4,096 | 4,096 | 99% | 4329.05 | 562.00 | 7.70x |
| 10,000 | 10,000 | 99% | 9754.66 | 1024.13 | 9.52x |
| 20,000 | 20,000 | 99% | 20588.34 | 2143.72 | 9.60x |

## Bloom filtering

Each false-positive estimate uses 100,000 absent SplitMix64 hashes.
Filter storage excludes the sorted hash array and allocation metadata.

| Blocks | Filter bytes | False positives | False-positive rate |
| --- | --- | --- | --- |
| 256 | 352 | 701 | 0.70% |
| 4,096 | 5,248 | 1,100 | 1.10% |
| 10,000 | 12,800 | 1,123 | 1.12% |
| 20,000 | 25,600 | 1,073 | 1.07% |

## Reproduction

```sh
python3 tests/bench_inflight.py --output target/inflight-splitmix
python3 tests/bench_inflight.py --blocks 30000 40000 50000 60000
cargo test -p kinference-fleet --release --lib lookup_with_stored_events -- --ignored --nocapture --test-threads=1
cargo test -p kinference-fleet --release --lib search_with_stored_events -- --ignored --nocapture --test-threads=1
cargo test -p kinference-fleet --release --lib partial_storage -- --ignored --nocapture --test-threads=1
cargo test -p kinference-fleet --release --lib stored_without_picks -- --ignored --nocapture --test-threads=1
cargo test -p kinference-fleet --release --lib bloom_filter_stats -- --ignored --nocapture --test-threads=1
```

The runner writes raw pick samples, RSS timelines and summaries under
the selected output directory. The SplitMix64 run uses
target/inflight-splitmix. The Cargo build currently requires cached
dependencies because the runner passes --offline.
