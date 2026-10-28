# Results: graph-query scheduling and concurrency

Completed local study: **`study-20261028T144619Z`**, 2026-10-28.
The [methodology](methodology.md) defines the experiment and measurement boundaries.
Raw evidence is in `results/study-20261028T144619Z` in the review bundle.

## What the evidence supports

Per-query round-robin protected small queries from large fan-outs in this
workload, at the cost of more large-query deadline misses. The configured
adaptive controller did not improve on the common calibrated fixed limit
under the injected response delay. Neither result establishes a universally
better policy.

There were six calibration runs and 24 measured runs: four configurations,
two offered rates, and three repetitions. All 19,200 measured arrivals have
recorded outcomes: 18,874 successes and 326 timeouts. No measured query was
rejected, queue-full, or failed with a storage/result error. Every success
was checked against the complete expected graph.

## Environment and protocol

Measurements ran in Docker on the existing local Ubuntu-22.04 WSL environment,
not on a dedicated remote VM. Cassandra and the Java service/load generator
shared the host, with separate container budgets.

| Component | Recorded setup |
| --- | --- |
| Cassandra | 5.0.5; one node; RF1; 4 CPUs / 4 GiB container limit |
| Benchmark | Java 21.0.7; 2 CPUs / 2 GiB container limit; 1 GiB maximum heap |
| Driver | DataStax Java driver 4.17.0; retries and speculative execution disabled |
| Adaptive limiter | Netflix concurrency-limits 0.5.4; WindowedLimit + Gradient2Limit |
| Dataset | 32 accounts; 3,232 profiles; 25,856 watched-title edges |
| Query sizes | 3 storage reads for small queries; 201 for large queries |
| Traffic | Regular open-loop arrivals; seeded 20% large-query selection |
| Measured arrivals | 20 seconds/run at 20 or 60 total queries/second |
| Disturbance | 30 ms delivery delay after real Cassandra completion, middle third |
| Deadline | 2 seconds from submission; queued work canceled, in-flight work drained |

Seed: `20261028`. All configurations received the same trace within a
rate/repetition block; execution order was deterministically shuffled.
Image identities, JAR SHA-256, parameters, and run order accompany the raw data.

The fixed limit was selected before measurement using healthy 40 qps traffic,
eight seconds per candidate, and an independent seed. All six candidates
completed 320/320 queries. The tie-breaker minimized the sum of small and large
successful-query p95 across both schedulers:

| Fixed limit | FIFO small / large p95, ms | Round-robin small / large p95, ms |
| --- | --- | --- |
| 8 | 239.24 / 299.25 | 3.57 / 29.12 |
| 20 | 3.40 / 15.64 | 3.18 / 14.69 |
| 64 | 3.28 / 10.22 | 3.02 / 9.44 |

This selected **64 for both schedulers**, also the adaptive initial and maximum
limit. It is finite healthy-workload calibration, not proof that 64 is optimal.
The candidate results show sensitivity, including a much worse FIFO result
at 8. Calibration was not repeated, so its variability is not estimated.

## Whole-run latency and completion

Each p95 cell is the **median of three per-run successful-query p95 values**,
with the observed min-max in parentheses, in milliseconds. These are not
pooled quantiles or confidence intervals. Completion counts are summed across
the three runs; the offered rate includes both query classes.

### 20 total queries/second

| Configuration | Small p95 ms | Large p95 ms | Small successes/offered | Large successes/offered |
| --- | --- | --- | --- | --- |
| Fixed / FIFO | 135.30 (132.51-136.31) | 235.53 (232.61-235.59) | 963/963 | 237/237 |
| Fixed / round-robin | 83.92 (82.53-88.33) | 249.30 (205.36-257.46) | 963/963 | 237/237 |
| Adaptive / FIFO | 172.79 (162.51-173.50) | 359.98 (299.05-468.87) | 963/963 | 237/237 |
| Adaptive / round-robin | 86.19 (85.10-88.65) | 348.07 (298.43-662.02) | 963/963 | 237/237 |

All queries completed. Round-robin reduced small-query p95 under either
limiter. Adaptive limiting did not improve the median p95 for either class
against the corresponding fixed configuration.

### 60 total queries/second

| Configuration | Small p95 ms | Large p95 ms | Small successes/offered | Large successes/offered |
| --- | --- | --- | --- | --- |
| Fixed / FIFO | 1322.25 (985.84-1396.20) | 1439.86 (1082.98-1713.96) | 2874/2878 | 719/722 |
| Fixed / round-robin | 113.99 (93.85-114.86) | 1601.51 (1562.51-1835.56) | 2878/2878 | 652/722 |
| Adaptive / FIFO | 1859.73 (1845.70-1888.86) | 1716.49 (1690.89-1802.22) | 2850/2878 | 645/722 |
| Adaptive / round-robin | 144.66 (123.75-148.16) | 1436.16 (1356.29-1678.84) | 2878/2878 | 578/722 |

With the same fixed capacity, changing FIFO to round-robin moved small-query
p95 from 1,322 ms to 114 ms (about an 11.6-fold ratio of medians). However,
large-query timeouts increased from **3/722 to 70/722** (0.4% to 9.7%).
Small-query timeouts fell from four to zero.

With adaptive limiting, round-robin also protected small queries, but
large-query timeouts increased from **77/722 to 144/722** (10.7% to 19.9%).
Its lower successful large-query p95 is not a clean improvement: more of the
slow queries failed and disappeared from the successful-latency population.

## Queue and limiter evidence

At 60 qps, median per-run p95 lookup queue wait was:

| Configuration | Small-query lookups, ms | Large-query lookups, ms |
| --- | --- | --- |
| Fixed / FIFO | 791.04 | 960.99 |
| Fixed / round-robin | 29.97 | 1296.96 |
| Adaptive / FIFO | 1172.08 | 1120.59 |
| Adaptive / round-robin | 43.25 | 1413.93 |

These are operation-level quantiles, not query latency, and include only
dispatched operations. Pending operations removed on timeout have no dispatch
sample. The queue evidence is consistent with the scheduling trade-off:
small requests obtained turns sooner, while large requests waited longer.

Across the 60 qps runs, sampled adaptive limits ranged from 38 to 64; fixed
limits stayed at 64. Median per-run p95 actual driver latency was 1.46-1.95 ms
across class/configuration groups, excluding the injected 30 ms delay.
The controlled latency increase therefore cannot be interpreted as measured
physical database overload. Reducing the slot budget in this response-delay
model can add queue pressure without removing the imposed latency.

Across all measured runs, observed maxima were 64 in-flight reads, 4,947
pending operations, and 122 active queries, within their 64 / 25,600 / 128
bounds. Analysis checked every logged dispatch against its dispatch-time
limit. Sampled state bounds are observations, not proof of every unsampled
instant; enforcement and timeout/cancellation behavior also have unit tests.
Maximum generator dispatch lag was 6.75 ms.

Docker samples spanning calibration and measurement peaked near 402% CPU for
Cassandra (approximately four cores) and 72% for the benchmark. Maximum
reported memory utilization was approximately 53% of Cassandra's 4 GiB budget
and 16% of the benchmark's 2 GiB budget. These coarse peaks do not establish
phase-specific bottlenecks or isolate unrelated host activity.

## Interpretation and limits

The useful distinction is **how many operations may run** versus **whose
operation runs next**. A concurrency controller alone did not provide query
fairness here. Per-query fairness also was not free: it changed which class
finished before a common deadline.

The setup uses a small, cache-friendly RF1 dataset on a shared machine.
Service and generator share one JVM, with no RPC transport, ingestion, cache
layer, replica failures, or production workload trace. The delay is added
after Cassandra completion, not by throttling disk, CPU, or network.

Twenty-second runs expose transient behavior, not steady-state controller
stability. There are only three measured repetitions. Every run/class group
has fewer than 1,000 successful samples; p99 is retained but not used for a
strong tail claim. Whole-run quantiles mix healthy, slowdown, and recovery
periods; `phases.json` offers arrival-based cohorts, which may execute across
phase boundaries. No evidence establishes Netflix-scale behavior or the
exact policy used inside Netflix RDG.

## Reproduce and inspect

From the project directory on Linux/WSL:

```bash
bash scripts/run-study.sh --seconds=20 --repeats=3 --rates=20,60 --calibration-seconds=8
python3 scripts/analyze.py results/study-20261028T144619Z
```

The first command creates a new timestamped study. The second reanalyzes this
recorded study when its raw evidence is present. Use the new directory to
analyze a reproduction; exact timings will vary.

See the recorded `report.md`, `summary.csv`, `phases.json`, and
[`latency.svg`](../results/study-20261028T144619Z/latency.svg), then inspect the
per-run CSVs rather than relying only on this interpretation. The local review
ZIP includes these artifacts and content checksums. No public repository,
article publication, or remote CI execution is implied.
