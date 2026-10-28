# Measured results

Shared-host Cassandra serving-path study. Medians and ranges below are across runs,
not confidence intervals. Latency quantiles include successes only; failure rates
are shown alongside them. Each class shares the same total offered query rate.

| Offered qps | Configuration | Class | Runs | Successful/offered | Success % (median) | p95 ms (median; min-max) |
| --- | --- | --- | --- | --- | --- | --- |
| 20 | FIFO_adaptive | large | 3 | 237/237 | 100.0 | 359.98; 299.05-468.87 |
| 20 | FIFO_adaptive | small | 3 | 963/963 | 100.0 | 172.79; 162.51-173.50 |
| 20 | FIFO_fixed | large | 3 | 237/237 | 100.0 | 235.53; 232.61-235.59 |
| 20 | FIFO_fixed | small | 3 | 963/963 | 100.0 | 135.30; 132.51-136.31 |
| 20 | ROUND_ROBIN_adaptive | large | 3 | 237/237 | 100.0 | 348.07; 298.43-662.02 |
| 20 | ROUND_ROBIN_adaptive | small | 3 | 963/963 | 100.0 | 86.19; 85.10-88.65 |
| 20 | ROUND_ROBIN_fixed | large | 3 | 237/237 | 100.0 | 249.30; 205.36-257.46 |
| 20 | ROUND_ROBIN_fixed | small | 3 | 963/963 | 100.0 | 83.92; 82.53-88.33 |
| 60 | FIFO_adaptive | large | 3 | 645/722 | 90.0 | 1716.49; 1690.89-1802.22 |
| 60 | FIFO_adaptive | small | 3 | 2850/2878 | 98.9 | 1859.73; 1845.70-1888.86 |
| 60 | FIFO_fixed | large | 3 | 719/722 | 100.0 | 1439.86; 1082.98-1713.96 |
| 60 | FIFO_fixed | small | 3 | 2874/2878 | 100.0 | 1322.25; 985.84-1396.20 |
| 60 | ROUND_ROBIN_adaptive | large | 3 | 578/722 | 81.5 | 1436.16; 1356.29-1678.84 |
| 60 | ROUND_ROBIN_adaptive | small | 3 | 2878/2878 | 100.0 | 144.66; 123.75-148.16 |
| 60 | ROUND_ROBIN_fixed | large | 3 | 652/722 | 90.0 | 1601.51; 1562.51-1835.56 |
| 60 | ROUND_ROBIN_fixed | small | 3 | 2878/2878 | 100.0 | 113.99; 93.85-114.86 |

Maximum observed generator dispatch lag: 6.75 ms.
Runs with fewer than 1,000 successful samples per class: 48/48.
Treat their p99 values as descriptive, not stable tail estimates.

The middle third adds 30 ms *after* actual Cassandra reads, holding permits
until response delivery. This is a dependency-response slowdown, not proof
of how physical Cassandra overload behaves. `driver_ms` excludes this delay.

See phases.json for arrival-cohort outcomes and state.csv for limit trajectories.
Short runs and one machine cannot establish general superiority or production capacity.
