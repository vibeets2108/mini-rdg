"""Verify raw measurements and generate tables/plots without third-party packages."""

import argparse
import csv
import html
import json
import math
import statistics
from collections import Counter, defaultdict
from pathlib import Path


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def query_stats(rows, duration):
    successes = [r for r in rows if r["status"] == "SUCCESS"]
    latencies = [float(r["latency_ms"]) for r in successes]
    elapsed = max(duration, max((float(r["end_ms"]) / 1000 for r in rows), default=0))
    return {
        "offered": len(rows),
        "success": len(successes),
        "failures": len(rows) - len(successes),
        "statuses": dict(Counter(r["status"] for r in rows)),
        "success_pct": 100 * len(successes) / len(rows) if rows else 0,
        "completed_per_second_including_drain": len(successes) / elapsed if elapsed else 0,
        "p50_ms": percentile(latencies, 0.50),
        "p95_ms": percentile(latencies, 0.95),
        "p99_ms": percentile(latencies, 0.99),
        "p99_samples_below_1000": len(successes) < 1000,
        "max_generator_lag_ms": max((float(r["lag_ms"]) for r in rows), default=0),
    }


def load_csv(path):
    with path.open(newline="") as stream:
        return list(csv.DictReader(stream))


def analyze(root):
    manifest = json.loads((root / "manifest.json").read_text())
    if manifest["status"] != "complete":
        raise ValueError("Study did not finish; cannot report it as complete")
    if len(set(manifest["selectedFixedLimits"].values())) != 1:
        raise ValueError("Schedulers must share the same calibrated fixed/initial limit")
    expected = manifest["settings"]["repeats"] * len(manifest["settings"]["rates"]) * 4
    if len(manifest["runOrder"]) != expected:
        raise ValueError("Missing or repeated configurations")
    traces = {}
    seen_configs = defaultdict(set)
    records = []
    phase_records = []
    for name in manifest["runOrder"]:
        directory = root / name
        meta = json.loads((directory / "run.json").read_text())
        rows = load_csv(directory / "queries.csv")
        if len(rows) != meta["rate"] * meta["seconds"]:
            raise ValueError(f"{name}: lost query outcomes")
        if len({r["id"] for r in rows}) != len(rows):
            raise ValueError(f"{name}: duplicate query outcomes")
        repeat = int(name.split("-")[0][1:])
        config = meta["config"]
        strategy = config["scheduler"] + ("_adaptive" if config["adaptive"] else "_fixed")
        key = (repeat, meta["rate"])
        if strategy in seen_configs[key]:
            raise ValueError(f"{name}: duplicate configuration")
        seen_configs[key].add(strategy)
        trace = sorted((int(r["id"]), r["class"], r["account"], float(r["scheduled_ms"])) for r in rows)
        if key in traces and traces[key] != trace:
            raise ValueError(f"{name}: unfair arrival schedule")
        traces[key] = trace
        query_classes = {r["id"]: r["class"] for r in rows}
        waits = defaultdict(list)
        calls = defaultdict(list)
        slots = defaultdict(list)
        with (directory / "operations.csv").open(newline="") as stream:
            for operation in csv.DictReader(stream):
                if int(operation["inflight_at_dispatch"]) > int(operation["limit_at_dispatch"]):
                    raise ValueError(f"{name}: dispatched above the concurrency limit")
                if int(operation["inflight_at_dispatch"]) > manifest["adaptive"]["max"]:
                    raise ValueError(f"{name}: exceeded hard maximum")
                cls = query_classes[operation["query"]]
                waits[cls].append(float(operation["queue_ms"]))
                calls[cls].append(float(operation["driver_ms"]))
                slots[cls].append(float(operation["slot_ms"]))
        state = load_csv(directory / "state.csv")
        if any(int(r["pending"]) > manifest["settings"]["maxPending"] for r in state):
            raise ValueError(f"{name}: queue bound exceeded")
        if any(int(r["active_queries"]) > manifest["settings"]["maxQueries"] for r in state):
            raise ValueError(f"{name}: admission bound exceeded")
        elapsed_seconds = max(
            meta["seconds"], max(float(r["end_ms"]) / 1000 for r in rows)
        )
        for cls in ("small", "large"):
            subset = [r for r in rows if r["class"] == cls]
            row = {
                "run": name, "repeat": repeat, "rate": meta["rate"], "strategy": strategy,
                "class": cls, **query_stats(subset, elapsed_seconds),
                "lookup_queue_p95_ms": percentile(waits[cls], 0.95),
                "driver_p95_ms": percentile(calls[cls], 0.95),
                "slot_p95_ms": percentile(slots[cls], 0.95),
                "min_limit": min(int(r["limit"]) for r in state),
                "max_limit": max(int(r["limit"]) for r in state),
            }
            records.append(row)
            for index, phase in enumerate(("healthy", "slowdown", "recovery")):
                # Cohorts are assigned by scheduled arrival, not completion.
                cohort = [r for r in subset if
                          index * meta["seconds"] * 1000 / 3 <= float(r["scheduled_ms"])
                          < (index + 1) * meta["seconds"] * 1000 / 3]
                phase_records.append({
                    "run": name, "rate": meta["rate"], "strategy": strategy, "class": cls,
                    "phase_by_arrival": phase, **query_stats(cohort, elapsed_seconds),
                })
    for configs in seen_configs.values():
        if len(configs) != 4:
            raise ValueError("Incomplete paired comparisons")
    (root / "summary.json").write_text(json.dumps(records, indent=2))
    (root / "phases.json").write_text(json.dumps(phase_records, indent=2))
    with (root / "summary.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)
    groups = defaultdict(list)
    for row in records:
        groups[(row["rate"], row["strategy"], row["class"])].append(row)
    lines = [
        "# Measured results",
        "",
        "Shared-host Cassandra serving-path study. Medians and ranges below are across runs,",
        "not confidence intervals. Latency quantiles include successes only; failure rates",
        "are shown alongside them. Each class shares the same total offered query rate.",
        "",
        "| Offered qps | Configuration | Class | Runs | Successful/offered | Success % (median) | p95 ms (median; min-max) |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    chart = []
    for (rate, strategy, cls), rows in sorted(groups.items()):
        p95 = [r["p95_ms"] for r in rows if r["p95_ms"] is not None]
        successes = sum(r["success"] for r in rows)
        offered = sum(r["offered"] for r in rows)
        median = statistics.median(p95) if p95 else None
        label = f"{median:.2f}; {min(p95):.2f}-{max(p95):.2f}" if p95 else "no successes"
        lines.append(
            f"| {rate} | {strategy} | {cls} | {len(rows)} | {successes}/{offered} "
            f"| {statistics.median(r['success_pct'] for r in rows):.1f} | {label} |"
        )
        if p95:
            chart.append((f"{rate} qps / {strategy} / {cls}", median, min(p95), max(p95)))
    max_lag = max(r["max_generator_lag_ms"] for r in records)
    lines += [
        "",
        f"Maximum observed generator dispatch lag: {max_lag:.2f} ms.",
        f"Runs with fewer than 1,000 successful samples per class: "
        f"{sum(r['p99_samples_below_1000'] for r in records)}/{len(records)}.",
        "Treat their p99 values as descriptive, not stable tail estimates.",
        "",
        "The middle third adds 30 ms *after* actual Cassandra reads, holding permits",
        "until response delivery. This is a dependency-response slowdown, not proof",
        "of how physical Cassandra overload behaves. `driver_ms` excludes this delay.",
        "",
        "See phases.json for arrival-cohort outcomes and state.csv for limit trajectories.",
        "Short runs and one machine cannot establish general superiority or production capacity.",
    ]
    (root / "report.md").write_text("\n".join(lines) + "\n")
    svg_chart(root / "latency.svg", chart)
    print("\n".join(lines))
    return records


def svg_chart(path, rows):
    width = 1100
    height = 80 + len(rows) * 32
    maximum = max((high for _, _, _, high in rows), default=1) or 1
    scale = 520 / maximum
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<g font-family="sans-serif" font-size="12" fill="#222">',
        '<text x="20" y="24">Successful-query p95 latency: median and min-max across runs (ms)</text>',
    ]
    for i, (label, median, low, high) in enumerate(rows):
        y = 55 + i * 32
        parts += [
            f'<text x="20" y="{y + 4}">{html.escape(label)}</text>',
            f'<line x1="{430 + low * scale:.2f}" y1="{y}" x2="{430 + high * scale:.2f}" y2="{y}" stroke="#777" stroke-width="2"/>',
            f'<circle cx="{430 + median * scale:.2f}" cy="{y}" r="4" fill="#1767a6"/>',
            f'<text x="970" y="{y + 4}">{median:.2f} ms</text>',
        ]
    parts.append("</g></svg>")
    path.write_text("\n".join(parts))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    analyze(parser.parse_args().directory)
