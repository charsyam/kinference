#!/usr/bin/env python3
"""Run isolated tracker benchmarks and sample process RSS on macOS or Linux."""

from __future__ import annotations

import argparse
import ctypes
import json
import math
import os
from pathlib import Path
import signal
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
BLOCK_COUNTS = [256, 4096, 10000, 20000, 30000, 40000, 50000, 60000]
MIB = 1024 * 1024


class TaskInfo(ctypes.Structure):
    # proc_taskinfo starts with two uint64_t fields and has a 96-byte ABI.
    _fields_ = [
        ("virtual_size", ctypes.c_uint64),
        ("resident_size", ctypes.c_uint64),
        ("remaining_fields", ctypes.c_byte * 80),
    ]


class ResidentMemory:
    def __init__(self):
        if sys.platform == "darwin":
            self.lib = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
            self.lib.proc_pidinfo.argtypes = [
                ctypes.c_int, ctypes.c_int, ctypes.c_uint64,
                ctypes.c_void_p, ctypes.c_int,
            ]
            self.lib.proc_pidinfo.restype = ctypes.c_int
        elif not sys.platform.startswith("linux"):
            raise RuntimeError("RSS sampling supports macOS and Linux")

    def read(self, pid):
        if sys.platform == "darwin":
            info = TaskInfo()
            size = self.lib.proc_pidinfo(pid, 4, 0, ctypes.byref(info), ctypes.sizeof(info))
            if size <= 0:
                return None
            if size != ctypes.sizeof(info):
                raise RuntimeError(f"unexpected proc_taskinfo size: {size}")
            return info.resident_size
        try:
            pages = int(Path(f"/proc/{pid}/statm").read_text().split()[1])
            return pages * os.sysconf("SC_PAGE_SIZE")
        except (FileNotFoundError, ProcessLookupError):
            return None


def build():
    result = subprocess.run(
        ["cargo", "test", "--release", "-p", "kinference-fleet", "--lib",
         "--offline", "--no-run", "--message-format=json"],
        cwd=ROOT, stdout=subprocess.PIPE, text=True, check=True,
    )
    for line in result.stdout.splitlines():
        message = json.loads(line)
        target = message.get("target", {}).get("name", "").replace("-", "_")
        if target == "kinference_fleet" and message.get("executable"):
            return Path(message["executable"])
    raise RuntimeError("cargo did not report a test executable")


def run_worker(binary, blocks, mode, repeat, output, memory, interval):
    stem = f"{blocks}-{mode}-{repeat}"
    log_path = output / f"{stem}.log"
    rss_path = output / f"{stem}-rss.jsonl"
    test = f"inflight::tests::contention::isolated_{mode}_{blocks}"
    command = [str(binary), "--exact", test, "--ignored", "--nocapture", "--test-threads=1"]
    samples = []
    phase = "setup"
    pending = ""
    began = time.monotonic()
    reaped = False
    with log_path.open("w") as log, log_path.open() as reader, rss_path.open("w") as timeline:
        pid = os.posix_spawn(str(binary), command, os.environ.copy(), file_actions=[
            (os.POSIX_SPAWN_DUP2, log.fileno(), 1),
            (os.POSIX_SPAWN_DUP2, log.fileno(), 2),
        ])
        try:
            while True:
                pending += reader.read()
                lines = pending.split("\n")
                pending = lines.pop()
                for line in lines:
                    if "BENCH_PHASE " in line:
                        phase = line.split("BENCH_PHASE ", 1)[1].strip()
                rss = memory.read(pid)
                if rss is not None:
                    sample = {"seconds": time.monotonic() - began, "phase": phase, "rss_bytes": rss}
                    samples.append(sample)
                    timeline.write(json.dumps(sample) + "\n")
                completed, status, usage = os.wait4(pid, os.WNOHANG)
                if completed:
                    reaped = True
                    if os.waitstatus_to_exitcode(status) != 0:
                        raise RuntimeError(f"benchmark failed: {log_path}\n{log_path.read_text()[-4000:]}")
                    break
                time.sleep(interval)
        finally:
            if not reaped:
                os.kill(pid, signal.SIGTERM)
                os.wait4(pid, 0)
    records = [json.loads(line.split("BENCH ", 1)[1])
               for line in log_path.read_text().splitlines() if "BENCH " in line]
    if len(records) != 1 or not records[0].get("samples_ns"):
        raise RuntimeError(f"missing raw pick samples: {log_path}")
    record = records[0]
    retained = [s["rss_bytes"] for s in samples if s["phase"] == "retained"]
    running = [s["rss_bytes"] for s in samples if s["phase"] == "running"]
    if not retained or not running:
        raise RuntimeError(f"missing RSS phases: {rss_path}")
    record.update({
        "implementation": mode,
        "repeat": repeat,
        "retained_rss_bytes": statistics.median(retained),
        "running_peak_rss_bytes": max(running),
        "process_peak_rss_bytes": usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024),
        "rss_samples": len(samples),
        "sample_interval_seconds": interval,
    })
    (output / f"{stem}.json").write_text(json.dumps(record) + "\n")
    return record


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def summarize(records):
    summary = []
    for blocks in sorted({r["blocks"] for r in records}):
        for mode in ["main", "current"]:
            runs = [r for r in records if r["blocks"] == blocks and r["implementation"] == mode]
            if not runs:
                continue
            samples = [sample for r in runs for sample in r["samples_ns"]]
            total = [s[0] / 1e6 for s in samples]
            wait = [s[2] / 1e6 for s in samples]
            held = [s[3] / 1e6 for s in samples]
            summary.append({
                "blocks": blocks, "implementation": mode, "runs": len(runs),
                "picks": len(total),
                "picks_per_second": len(total) / sum(r["elapsed_seconds"] for r in runs),
                "p50_ms": percentile(total, 0.50), "p99_ms": percentile(total, 0.99),
                "max_ms": max(total),
                "min_picks_per_thread": min(min(r["per_thread"]) for r in runs),
                "wait_p50_ms": percentile(wait, 0.50), "wait_p99_ms": percentile(wait, 0.99),
                "held_mean_ms": statistics.mean(held),
                "held_p50_ms": percentile(held, 0.50), "held_p99_ms": percentile(held, 0.99),
                "retained_rss_mib": statistics.median(r["retained_rss_bytes"] for r in runs) / MIB,
                "running_peak_rss_mib": max(r["running_peak_rss_bytes"] for r in runs) / MIB,
                "process_peak_rss_mib": max(r["process_peak_rss_bytes"] for r in runs) / MIB,
            })
    return summary


def write_summary(output, records):
    summary = summarize(records)
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    suite = []
    for mode in ["main", "current"]:
        runs = [r for r in records if r["implementation"] == mode]
        if not runs:
            continue
        total = [sample[0] / 1e6 for r in runs for sample in r["samples_ns"]]
        suite.append({
            "implementation": mode, "picks": len(total),
            "picks_per_second": len(total) / sum(r["elapsed_seconds"] for r in runs),
            "p50_ms": percentile(total, 0.5), "p99_ms": percentile(total, 0.99),
            "max_ms": max(total),
            "running_peak_rss_mib": max(r["running_peak_rss_bytes"] for r in runs) / MIB,
        })
    (output / "suite-summary.json").write_text(json.dumps(suite, indent=2) + "\n")
    lines = [
        "# Isolated in-flight benchmark results", "",
        "Each case runs in a fresh process. Counts describe block hashes.",
        "Throughput pools completed picks and elapsed time across runs.",
        "Held time measures tracker work under the pool mutex and excludes waiting.",
        "RSS includes the fixture, tracker, threads and benchmark buffers.",
        "Retained RSS is the median before picking. Running peak is the largest sampled RSS during picking.",
        "Process peak is the kernel high-water mark, including validation and output serialization.",
        "The harness excludes gRPC, tokenization, scoring and policy selection.", "",
        "## Throughput, held time and resident memory", "",
        "| Blocks | Implementation | Picks | Picks/s | Held mean ms | Retained MiB | Running peak MiB | Process peak MiB |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in summary:
        lines.append(
            f'| {row["blocks"]:,} | {row["implementation"]} | {row["picks"]:,} | '
            f'{row["picks_per_second"]:.2f} | {row["held_mean_ms"]:.3f} | '
            f'{row["retained_rss_mib"]:.2f} | {row["running_peak_rss_mib"]:.2f} | '
            f'{row["process_peak_rss_mib"]:.2f} |'
        )
    lines.extend([
        "", "## Queue-inclusive latency diagnostics", "",
        "Latency includes query preparation, mutex wait and tracker reads.",
        "Mutex admission can concentrate completed picks on one thread.",
        "The latency percentiles do not isolate tracker cost.",
        "Percentiles pool all completed picks across threads and runs.",
        "Min picks/thread is the smallest completion count of one thread in any run.", "",
        "| Blocks | Implementation | Picks | Min picks/thread | p50 ms | p99 ms | Max ms | Wait p99 ms |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ])
    for row in summary:
        lines.append(
            f'| {row["blocks"]:,} | {row["implementation"]} | {row["picks"]:,} | '
            f'{row["min_picks_per_thread"]:,} | {row["p50_ms"]:.3f} | '
            f'{row["p99_ms"]:.3f} | {row["max_ms"]:.3f} | {row["wait_p99_ms"]:.3f} |'
        )
    (output / "summary.md").write_text("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--blocks", type=int, nargs="+", choices=BLOCK_COUNTS, default=BLOCK_COUNTS)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--sample-ms", type=float, default=20.0)
    parser.add_argument("--output", type=Path, default=ROOT / "target/inflight-isolated")
    args = parser.parse_args()
    if args.repeats < 1 or args.sample_ms <= 0:
        parser.error("repeats and sample-ms must be positive")
    args.output.mkdir(parents=True, exist_ok=True)
    memory = ResidentMemory()
    binary = build()
    records = []
    for repeat in range(args.repeats):
        for blocks in args.blocks:
            modes = ["main", "current"] if repeat % 2 == 0 else ["current", "main"]
            for mode in modes:
                print(f"running blocks={blocks} implementation={mode} repeat={repeat}", flush=True)
                record = run_worker(binary, blocks, mode, repeat, args.output, memory, args.sample_ms / 1000)
                records.append(record)
                write_summary(args.output, records)
                print(f'completed picks={record["total"]["n"]} rate={record["picks_s"]:.2f} '
                      f'running_peak_mib={record["running_peak_rss_bytes"] / MIB:.2f}', flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
