# Graph-query concurrency study

## Question and scope

How should a service share a bounded number of storage operations between
small and large graph queries, particularly during dependency slowdowns?

This is one factorial study, not a reconstruction of Netflix internals:

| Scheduling | Fixed limit | Adaptive limit |
| --- | --- | --- |
| Global FIFO | A | B |
| Per-query round-robin | C | D |

The adaptive algorithm is Netflix concurrency-limits 0.5.4's `Gradient2Limit`,
wrapped in `WindowedLimit`. The scheduler and its queues are ours. These
techniques are established; the contribution is a reproducible, workload-specific
comparison, not a new algorithm.

## Actual system

The benchmark runs the graph-query service core and open-loop client in one JVM,
with real asynchronous Cassandra requests over the Compose network. It does
not include gRPC or HTTP overhead. The same service core is used for every
configuration. That isolates the storage-dispatch boundary, but is not an
end-to-end production RPC benchmark.

The generator is on its own thread; service state is owned by a single event
loop. Futures notify that loop without blocking it on network I/O. CPU
saturation of either loop can distort results and must be considered alongside
generator lag and resource telemetry.

Dataset:

- 16 small accounts with 2 profiles each.
- 16 large accounts with 200 profiles each.
- 8 watched-title edges per profile.
- A query performs one account adjacency read, then one read per profile.
- Every read fits in one 512-row driver page; unexpected pagination fails.
- Both tables have `PRIMARY KEY (source, target)`.
- RF=1 on one Cassandra node: no replica-failure or distributed-scale claims.
- The dataset is small and deliberately reusable/cache-friendly. Similar row
  counts do not guarantee identical actual service times.

The 200-profile accounts are synthetic fan-out, not Netflix product semantics.
The response contains the complete profile-to-title adjacency map. Every
successful measured response is compared with the seeded expected result.

## Scheduling and admission

Root lookups and all subsequent adjacency lookups enter the same scheduler.
FIFO preserves operation enqueue order. Round-robin takes one operation from
each ready query in turn. Neither preempts operations already dispatched.
Giving queries equal turns is not the same as equal CPU or I/O cost.

Defaults:

- At most 128 admitted queries, including expired queries with unfinished I/O.
- At most 25,600 pending operations; an expansion that cannot fit is rejected
  as a whole, not partially reported as successful.
- Two-second service deadline, starting on submission; scheduled-to-completion
  latency additionally includes generator dispatch lag.
- Query timeout/cancellation removes queued work. In-flight reads drain.
- A slot is released on terminal driver completion plus any explicitly injected
  delivery delay, not merely when the query's deadline expires.
- A driver timeout cannot prove that Cassandra has stopped executing the
  request. This limitation still exists; speculative execution and retries are
  disabled, and the driver timeout is 30 seconds.
- Successful, rejected, queue-full, timed-out, and failed outcomes are recorded.

The limit is per service instance, not cluster-wide. Adaptive shrinkage can
temporarily leave more outstanding operations than the newly reduced limit;
no new operation dispatches until capacity becomes available. The invariant
checked in analysis is in-flight count versus the limit **at dispatch**.

## Adaptive policy and fixed calibration

Adaptive settings are recorded in every manifest:

- 500 ms averaging windows; at least 10 qualifying samples per window.
- Minimum 4, maximum 64, initial value equal to the common calibrated fixed limit.
- Gradient2 long window 20, tolerance 1.5, smoothing 0.2, queue-size term 4.
- RTT sample spans dispatch to completed delivery, excluding our scheduler
  queue wait and including the controlled delivery delay.

Do not describe Gradient2 as a generic error-triggered backoff controller:
its implementation is RTT-based and does not directly use `didDrop` in the
gradient calculation. We record errors separately.

The RDG article illustrates gradual growth and a larger reduction when
timeouts/errors spike. It does not identify this exact Gradient2 setup.
Choosing a documented public RTT-based implementation is our adaptation,
not evidence that Netflix's RDG uses it.

Before measured runs, each scheduler calibrates fixed limits 8, 20, and 64 at
40 queries/second with no injected slowdown and a different random seed.
One common limit is selected for both schedulers. Selection maximizes successful
queries summed across schedulers, breaking ties by the sum of successful
small-query and large-query p95 latency across schedulers, then the lower limit.
Calibration results are
retained, including losing candidates.

This is a finite healthy-workload tuning rule, not an oracle or proof of the
optimal fixed limit. Sharing one selected fixed limit and adaptive initial
limit avoids confounding scheduling with different starting capacity budgets.
The raw candidate runs also allow equal-limit sensitivity comparisons.

## Traffic and slowdown

Regular arrivals are scheduled independently of completion. A seeded generator
selects 20% large queries and a uniformly selected account within each class.
All four configurations receive identical traces for a rate/repeat pair.
Do not call this a Poisson or production trace.

Default measured rates are 20 and 60 queries/second, 20-second arrival windows,
with three repetitions. Configuration order is seeded and shuffled per block.
Each run drains fully before the next. Warm-up reads are identical per run;
the measured limiter is reset. A real-Cassandra validation pass warms the
process before calibration. JIT/cache drift and host variability remain.

For requests dispatched in the middle third of a measured run, delivery is
delayed by 30 ms *after the real Cassandra request completes*. Its permit stays
occupied through the delay. This models a suddenly slower dependency response
without requiring privileged network manipulation.

This is not physical Cassandra CPU, disk, or replica overload. `driver_ms`
separates actual call time from `slot_ms`, which includes injection and callback
dispatch. A decline in performance under injected delay does not prove that the
same controller behaves identically under real database saturation.

## Evidence and analysis

Each run writes:

- `queries.csv`: scheduled/start/end times, generator lag, class, status,
  scheduled-to-complete latency, and service latency.
- `operations.csv`: root/leaf, queue wait, actual driver time, slot occupancy,
  in-flight count and limit at dispatch, dependency failures.
- `state.csv`: 100 ms samples of current limit, in-flight work, pending work,
  active queries, and heap usage.
- `run.json`: configuration and run summary.

The suite also retains manifest, run order, fixed calibration, resolved Compose
configuration, image identities, and Docker resource samples.

`scripts/analyze.py` verifies paired traces, outcome conservation, complete
configuration coverage, and observed resource bounds before generating tables
and an SVG. It reports per-run quantiles, medians and ranges across repetitions,
not pseudo-confidence intervals computed from non-independent requests.
Arrival-based phase cohorts can overlap phases in execution time.

Per-lookup wait quantiles describe dispatched operations; canceled pending
operations have no dispatch sample. Their query outcomes remain in the query
CSV. Per-class completion rates use the same whole-run elapsed denominator
including drain, not a separate favorable window for each class. Phase-cohort
rates also use that whole-run denominator; they are not instantaneous phase
throughput.

Successful-query latency is conditional on success. Always read it alongside
timeouts and rejections. Samples with fewer than 1,000 successful queries per
class are flagged for unstable p99 interpretation; that threshold itself is
not a statistical guarantee. Short runs are transient observations, not proof
of equilibrium or long-term controller stability.

## Prior work and attribution

- [Netflix RDG Part 3](https://netflixtechblog.com/how-and-why-netflix-built-a-real-time-distributed-graph-part-3-querying-the-graph-with-grpc-0f3468349607):
  motivation for fan-out, asynchronous execution, and bounded/adaptive work.
- [Netflix concurrency-limits](https://github.com/Netflix/concurrency-limits):
  existing limiter implementation, algorithms, and partitioning concepts.
- [Envoy adaptive concurrency](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/adaptive_concurrency_filter):
  an existing production proxy implementation with its own measurement and
  sampling choices. We do not implement or benchmark Envoy here.

These sources establish that adaptive concurrency and fairness are not new.
The repository does not establish that Netflix uses this exact scheduler,
Gradient2 configuration, schema, dataset, or failure model.

## Publication boundary

Local measurements and source-derived explanations must be labeled separately.
One shared machine with constrained containers cannot establish fleet behavior,
independent failure domains, cold-cache disk costs, or Netflix-scale capacity.
No claimed hiring outcome or company endorsement follows from the work.
