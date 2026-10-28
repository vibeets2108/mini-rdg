# A Concurrency Limit Is Not a Scheduling Policy

*Sharing storage capacity between small and large graph queries.*

Reading [Netflix's series on real time distributed graphs](https://netflixtechblog.com/how-and-why-netflix-built-a-real-time-distributed-graph-part-1-ingesting-and-processing-data-80113e124acc) made me wonder how a
service should share storage capacity between queries of very different sizes.

*Written with AI assistance.*

Suppose one query needs three storage reads and another needs 201. The service
allows 64 reads at a time. If the large query fills the queue first, how long
does the small query have to wait?

I built a small benchmark using Cassandra to compare two scheduling policies
under fixed and adaptive concurrency limits. With the fixed limit held
constant, round robin scheduling reduced tail latency for small queries, but more
large queries missed their deadlines. Choosing between those outcomes depends
on which queries the service needs to protect.

## Two decisions at the storage queue

Netflix's [RDG Part 3](https://netflixtechblog.com/how-and-why-netflix-built-a-real-time-distributed-graph-part-3-querying-the-graph-with-grpc-0f3468349607)
discusses asynchronous graph querying and controlling concurrent work.
Rather than reproduce the entire ingestion and serving architecture, I
isolated one boundary: dispatching storage reads.

The concurrency limit controls how many reads may be outstanding. The
scheduler chooses which ready query gets the next available slot.

FIFO (first in, first out) preserves the order in which storage operations
enter a shared queue.
Round robin takes one operation from each ready query in turn. Neither
preempts a read already in progress. Equal turns also do not guarantee equal
CPU or I/O cost.

For a small illustration, suppose query A has four reads queued, followed by
one each from B and C. With all six reads ready before dispatch, the policies
produce these dispatch orders:

```text
FIFO:         A1 A2 A3 A4 B1 C1
Round robin:  A1 B1 C1 A2 A3 A4
```

This is an illustration, not a measured trace. B and C get earlier turns
under round robin, while A's remaining reads move later. Reads can overlap,
so dispatch order is not completion order.

I compared fixed/FIFO, fixed/round robin, adaptive/FIFO, and
adaptive/round robin against the same query traces.

For the adaptive policy, I used the existing `Gradient2Limit` implementation
from [Netflix's concurrency limits library](https://github.com/Netflix/concurrency-limits),
behind its windowed sampling wrapper. It adjusts the limit using round trip
time measurements. Here, those samples include the injected response delay
but exclude time waiting in the scheduler queue. The RDG article describes
adjustment behavior involving errors and timeouts. It does not establish
that RDG uses this exact Gradient2 configuration.
That implementation choice is mine, not a claim about Netflix's internals.

Neither adaptive concurrency nor round robin is a new idea.
[Envoy's adaptive concurrency filter](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/adaptive_concurrency_filter)
is another example of existing work, with different measurement choices.
The comparison below uses the Netflix library with the two schedulers.

## What the benchmark measured

The query shape was:

```text
Account -> Profiles -> Watched titles
```

A small query read two profiles; a large query read 200. Including the
account adjacency lookup, those queries required three and 201 storage reads.
Every profile had eight title edges. These are synthetic query sizes,
not Netflix account semantics.

The Java service core made real asynchronous reads against Cassandra.
The load generator shared the service JVM; Cassandra ran in a separate
container on the same host. There was no gRPC endpoint, ingestion pipeline,
or application cache in this benchmark.

All four configurations used the same seeded graph and scheduled arrivals.
Traffic was open loop: arrivals did not wait for earlier queries to complete.
Each trace selected large queries with 20% probability.

Before the measured runs, I tested fixed limits of 8, 20, and 64 with each
scheduler. Each calibration run lasted eight seconds at 40 queries per second,
using a separate calibration seed shared across all six candidates and no
injected slowdown.

The selection rule first favored the limit that completed the most queries
across both schedulers. If completion counts tied, it favored the lowest sum
of the 95th percentile latencies, or p95, for small and large queries under
both schedulers. A remaining tie would favor the lower limit.

Every calibration run completed all 320 queries. The limit of 64 had the
lowest combined p95, so both schedulers used 64 in the fixed comparison.
Adaptive runs also started at 64 and could vary between 4 and 64. This kept
the initial and maximum capacity budgets equal. Calibration was performed
once per candidate and scheduler; it does not establish an optimal limit
for every workload.

The measured suite used 20 and 60 total queries per second, arrival windows
of 20 seconds, and three repetitions of each configuration. Execution order was
shuffled within each rate/repetition block.
That produced 24 measured runs. Each run used the same warmup reads and
drained all outstanding work before the next run began.

During the middle third of each run, I added 30 ms to response delivery
after Cassandra completed each read. The storage permit remained occupied
until delivery. That is a controlled delay in dependency responses, not
physical Cassandra saturation.

This setup isolates the effect of longer slot occupancy. It does not measure
how Cassandra behaves when its CPU, disk, or network is throttled.

Every successful response was compared against the complete expected graph.
The service admitted at most 128 queries and queued at most 25,600 pending
storage operations. Every query had a deadline of two seconds from submission.
On timeout, queued work was removed, but work already dispatched
kept its permits until completion. Otherwise, expiring requests could
silently cause the service to exceed its intended concurrency.

The suite recorded every query outcome, lookup queue wait, actual driver
duration, slot occupancy, dispatch limit, and sampled queue state. Analysis
checked paired traces and dispatch bounds before generating the report.

There were 19,200 measured queries: 18,874 successes and 326 timeouts.
No measured query was rejected at admission or because the pending queue was
full. There were no storage errors or incorrect results. Timeouts remain in
the outcome counts; they are not included in
successful latency percentiles.

## The result: small queries gained, large queries paid

At 60 total queries per second:

| Configuration | Small query p95 | Large query p95 | Small timeouts | Large timeouts |
| --- | --- | --- | --- | --- |
| Fixed / FIFO | 1322 ms | 1440 ms | 4 / 2878 | 3 / 722 |
| Fixed / round robin | 114 ms | 1602 ms | 0 / 2878 | 70 / 722 |
| Adaptive / FIFO | 1860 ms | 1716 ms | 28 / 2878 | 77 / 722 |
| Adaptive / round robin | 145 ms | 1436 ms | 0 / 2878 | 144 / 722 |

For each run, p95 is the latency at the 95th percentile of successful queries.
The table reports the median of those three values, not a percentile
pooled across runs. Timeout counts are totals across the three runs; each
denominator is the number of queries offered in that class.

Holding the fixed limit constant, round robin moved p95 for small queries from
about 1.32 seconds to 114 milliseconds. Yet deadline misses for large queries
increased from 0.4% to 9.7%.

Across the three repetitions, p95 for small queries ranged from 986 to 1396 ms
with fixed/FIFO and from 94 to 115 ms with fixed/round robin. These are
observed ranges, not confidence intervals.

The queue measurements helped explain the tradeoff. For lookups belonging to
small queries, p95 wait fell from a median of 791 ms under fixed/FIFO to 30 ms
under fixed/round robin. For lookups belonging to large queries, p95 wait rose
from 961 ms to 1297 ms. These measurements describe individual lookups, not
the full duration of a query.

These measurements are consistent with the queue example: taking turns lets
small queries make progress while large queries still have work waiting.
The cost to large queries matters because all queries have the same deadline.

At 20 total queries per second, all queries completed. With fixed limiting,
p95 for small queries fell from 135 ms with FIFO to 84 ms with round robin.
The result at this lower load was less dramatic, but pointed in the same direction.

## What happened with the adaptive limit

At 60 qps, sampled adaptive limits ranged from 38 to 64. The adaptive
configurations had more timeouts and higher p95 for small queries than their
fixed counterparts.

The slowdown model helps interpret this. Actual driver p95 latency, summarized
across class/configuration groups, remained around 1.5 to 2 ms; the extra 30 ms
was deliberately added afterward. Reducing concurrency could add waiting
without eliminating that externally imposed response delay.

That is a plausible explanation for these results, not proof of the cause.
A physically overloaded database might respond differently when concurrency
falls. This benchmark doesn't test that case.

There is another reason to read latency and failures together.
Adaptive/round robin has a lower p95 for successful large queries than
adaptive/FIFO, but almost twice as many large queries timed out.
More slow queries fell out of the successful population.
Reporting only that percentile would tell a misleading story.

## What this study cannot establish

These measurements used one shared WSL/Docker host, one Cassandra node with
replication factor 1, and a small dataset that favors caching.
Cassandra's container was limited to four CPUs and 4 GiB of memory. The
service and generator shared a JVM in a container limited to two CPUs and
2 GiB. Those limits did not provide independent failure domains. The benchmark
used Java 21, Cassandra 5.0.5, and version 0.5.4 of Netflix's concurrency limits
library, and excluded RPC overhead.

Runs of 20 seconds and three repetitions describe short transients, not
controller stability over longer periods. Every run/class group had fewer than 1,000
successful samples, so I have not built a strong p99 claim around them.
The reported latency percentiles mix healthy, slowdown, and recovery periods.
Data grouped by arrival phase is retained separately.

The results don't establish capacity at Netflix's scale or a general advantage
for either policy.

## Choosing which queries to protect

If a service prioritizes small interactive queries, these results give a
reason to evaluate scheduling that gives each query a turn. If large queries must meet the same
deadline, the additional failures may be unacceptable. At the higher offered
rate, fixed/FIFO completed more queries overall than fixed/round robin, even
though its latency for small queries was much worse.

The limit stayed at 64 in both cases. Changing who received the next slot
changed which queries completed in time. A service owner still has to decide
whether that exchange serves the workload; a lower latency number alone
cannot make the decision.

## Evidence and reproduction

The code, complete configuration, and raw measurements are in
the [companion GitHub repository](https://github.com/vibeets2108/mini-rdg).
The [reviewed source snapshot](https://github.com/vibeets2108/mini-rdg/tree/aeaae381dcaa23de7252752c4b3baaea5a5e5f13)
and [evidence release](https://github.com/vibeets2108/mini-rdg/releases/tag/study-20261028-review2)
support reproducing the experiment and
inspecting individual outcomes beyond the results presented here. The evidence
bundle contains source, raw data from each run, and content checksums for
study `study-20261028T144619Z`.

*Published on [Medium](https://medium.com/@vineetsh2108/a-concurrency-limit-is-not-a-scheduling-policy-ccb11b3d1e12)
on 2026-10-28. This source copy retains the results table; the Medium version
presents the same values in labeled text blocks. The repository's MIT license
excludes this article text, whose rights are retained.*
