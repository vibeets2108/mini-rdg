# Mini-RDG

Mini-RDG is a focused investigation of graph-query concurrency, motivated by
the Netflix TechBlog RDG series.

The completed 24-run local study found that per-query round-robin protected
small queries during an injected response slowdown, at the cost of more
large-query timeouts. The configured adaptive limiter did not improve the
calibrated fixed baseline in this setup. See the [evidence](docs/results.md)
and [article draft](docs/article-draft.md), including their limitations.

## Study scope

How should a graph-query service control and share storage concurrency when
small and large queries compete, especially when storage slows down?

One completed study compares:

| Scheduling | Fixed limit | Adaptive limit |
| --- | --- | --- |
| FIFO | A | B |
| Round-robin across queries | C | D |

Use the same prepared Cassandra graph, query semantics, arrival traces,
admission bounds, and resource budgets. Use an existing adaptive algorithm.
The benchmark must distinguish queue waiting, storage-call time, complete-query
latency, successful throughput, rejections, and timeouts by query size.

The implementation includes a load generator, a graph-query service core,
replaceable scheduling/limiting strategies, and Cassandra. Streaming ingestion,
a frontend, application caching, and a full Netflix replica are out of scope.

## Run it

Requirements: Linux (or WSL2), Docker Engine with Compose, Python 3, and enough
free resources for the declared container budgets. Cassandra is capped at
4 CPUs/4 GB; the benchmark at 2 CPUs/2 GB. No host JDK or Maven is required.
Images and Maven dependencies require network access on the first run.

From the project directory in a Linux/WSL terminal:

```bash
bash scripts/run-study.sh
```

From Windows PowerShell with this checkout and the existing Ubuntu distribution:

```powershell
wsl.exe -d Ubuntu-22.04 --cd C:\work\mini-rdg -- bash scripts/run-study.sh
```

Adapt the checkout path and distribution name to your machine. The script:

1. Builds the Java artifact, runs unit tests and formatting checks.
2. Starts a private, resource-limited Cassandra container (no host ports).
3. Seeds synthetic data and checks complete results under all four strategies.
4. Calibrates fixed limits, runs 24 measured comparisons, and retains raw data.
5. Verifies the measurements, generates a report and SVG, and stops Cassandra.

Default measurements take about nine minutes plus warm-up, build, and startup.
The first dependency/image download can take longer. Data is retained in the
project's named Docker volume. No other containers or databases are changed.
Do not run two suites concurrently against the same Compose project.

For a shorter infrastructure check (not publication evidence):

```bash
bash scripts/run-study.sh --seconds=6 --repeats=1 --rates=20 --calibration-seconds=3
```

## What is actually measured?

The graph service core and open-loop client share a JVM; actual asynchronous
Cassandra reads cross the container network. This does **not** measure gRPC
transport or a full ingestion system.

A controlled 30 ms delay is added to response delivery in the middle third of
each run, after Cassandra finishes. This holds a slot without blocking a Java
thread. Actual driver latency and total slot occupancy are logged separately.
It models a dependency-response slowdown, not physical Cassandra saturation.

See [methodology](docs/methodology.md) for all contracts, calibration rules,
known limitations, and prior work. See [results](docs/results.md) and the
[article draft](docs/article-draft.md) for the recorded local study.

## Inspect the evidence

The recorded study is
[`study-20261028T144619Z`](results/study-20261028T144619Z/report.md).
Its summaries, chart, manifest, and environment measurements are tracked here
so the findings can be inspected without running the benchmark.

The [study evidence release](https://github.com/vibeets2108/mini-rdg/releases/tag/study-20261028)
contains the full review ZIP, including raw per-query and per-operation data.
The repository and release are private during review. A GitHub sign-in with
access to this repository is required.

To reanalyze the recorded runs without starting containers, download the review
ZIP from that release and extract it into a separate directory. Run the analyzer
from the extracted project's root:

```bash
python3 scripts/analyze.py results/study-20261028T144619Z
```

The archive includes code as well as data; extracting separately avoids
overwriting changes in an existing checkout.

Each `results/study-TIMESTAMP` directory contains:

```text
manifest.json             versions, settings, calibration, order, completion status
container-stats.jsonl      CPU, memory, network and block-I/O samples
RUN/queries.csv            every offered query and its outcome
RUN/operations.csv         per-lookup waits, service times and dispatch limits
RUN/state.csv              queue depth, active queries and limiter trajectory
summary.csv / summary.json per-run and per-class measurements
phases.json                healthy/slowdown/recovery arrival cohorts
report.md / latency.svg    generated tables and chart
```

Reanalyze without running containers:

```bash
python3 scripts/analyze.py results/study-TIMESTAMP
```

The analyzer fails on lost outcomes, unequal paired traces, incomplete
configuration coverage, or observed dispatch/admission/queue bound violations.
Raw per-run results are ignored by Git because they can be large; the selected
study's summaries are tracked, and its complete raw data is retained in the
reviewed evidence bundle.

Prepare a local review ZIP after analysis:

```bash
python3 scripts/package_evidence.py results/study-TIMESTAMP
```

It includes implementation, current methodology, article draft, raw results,
and SHA-256 checksums. It excludes historical session notes and resolved Compose
files containing machine-specific absolute paths. Creating this archive does
not upload or publish anything.

## Development

With JDK 21 and Maven installed, run `mvn clean verify`. Alternatively:

```bash
docker run --rm --cpus=2 --memory=2g \
  -v "$PWD:/work" -v mini-rdg-study-maven-cache:/root/.m2 -w /work \
  maven:3.9.9-eclipse-temurin-21 mvn -B clean verify
python3 -m unittest discover -s scripts -p 'test_*.py'
```

Format Java with `mvn spotless:apply` using the same environment. To stop only
this project's database manually, use `docker compose stop cassandra`.

No public publication has been performed. Publication and license selection
require owner approval. Do not claim Netflix-scale capacity or undisclosed
Netflix internals from these local measurements.

## Source articles

- [Part 1: Ingesting and Processing Data Streams at Internet Scale](https://netflixtechblog.com/how-and-why-netflix-built-a-real-time-distributed-graph-part-1-ingesting-and-processing-data-80113e124acc)
- [Part 2: Building a Scalable Storage Layer](https://netflixtechblog.com/how-and-why-netflix-built-a-real-time-distributed-graph-part-2-building-a-scalable-storage-layer-ff4a8dbd3d1f)
- [Part 3: Querying the Graph with gRPC](https://netflixtechblog.com/how-and-why-netflix-built-a-real-time-distributed-graph-part-3-querying-the-graph-with-grpc-0f3468349607)
