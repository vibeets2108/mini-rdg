package study.rdg;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.limit.FixedLimit;
import com.netflix.concurrency.limits.limit.Gradient2Limit;
import com.netflix.concurrency.limits.limit.WindowedLimit;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public final class Main {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final int MAX_QUERIES = 128;
  private static final int MAX_PENDING = 25600;
  private static final int MAX_LIMIT = 64;
  private static final Duration DEADLINE = Duration.ofSeconds(2);

  private record Config(WorkQueue.Mode scheduler, boolean adaptive, int fixed) {
    String name() {
      return scheduler.name().toLowerCase() + "-" + (adaptive ? "adaptive" : "fixed-" + fixed);
    }
  }

  private record Arrival(int id, String account, long offset) {}

  private record RunResult(int offered, int success, double smallP95, double largeP95, int peak) {}

  static Limit limit(boolean adaptive, int fixed) {
    if (!adaptive) return FixedLimit.of(fixed);
    return WindowedLimit.newBuilder()
        .minWindowTime(500, TimeUnit.MILLISECONDS)
        .maxWindowTime(500, TimeUnit.MILLISECONDS)
        .windowSize(10)
        .build(
            Gradient2Limit.newBuilder()
                .initialLimit(fixed)
                .minLimit(4)
                .maxConcurrency(MAX_LIMIT)
                .longWindow(20)
                .rttTolerance(1.5)
                .smoothing(0.2)
                .queueSize(4)
                .build());
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) throw new IllegalArgumentException("Expected seed, validate, or study");
    Map<String, String> options = new LinkedHashMap<>();
    for (int i = 1; i < args.length; i++) {
      String[] parts = args[i].split("=", 2);
      if (parts.length != 2
          || !List.of(
                  "--output",
                  "--seconds",
                  "--repeats",
                  "--rates",
                  "--seed",
                  "--calibration-seconds")
              .contains(parts[0])) {
        throw new IllegalArgumentException("Unknown option: " + args[i]);
      }
      options.put(parts[0], parts[1]);
    }
    String host = System.getenv().getOrDefault("CASSANDRA_HOST", "127.0.0.1");
    try (CassandraStore store = new CassandraStore(host)) {
      switch (args[0]) {
        case "seed" -> {
          store.seed();
          System.out.println("Seeded 32 accounts, 3232 profiles, 25856 watched edges.");
        }
        case "validate" -> validate(store);
        case "study" -> study(store, options);
        default -> throw new IllegalArgumentException("Expected seed, validate, or study");
      }
    }
  }

  private static void validate(Store store) throws Exception {
    for (WorkQueue.Mode mode : WorkQueue.Mode.values()) {
      for (boolean adaptive : List.of(false, true)) {
        try (GraphEngine engine =
            new GraphEngine(
                store,
                mode,
                limit(adaptive, 20),
                MAX_QUERIES,
                MAX_PENDING,
                Duration.ofSeconds(20),
                ignored -> 0,
                null,
                System.nanoTime())) {
          List<CompletableFuture<Void>> requests = new ArrayList<>();
          for (int i = 0; i < 16; i++) {
            String account = Dataset.account(i % 4 == 0, i % Dataset.ACCOUNTS_PER_CLASS);
            requests.add(
                engine
                    .submit(i, account, System.nanoTime())
                    .thenAccept(
                        outcome -> {
                          if (!outcome.status().equals("SUCCESS")
                              || !outcome.result().equals(Dataset.expected(account))) {
                            throw new IllegalStateException(
                                "Incorrect traversal for " + account + ": " + outcome.status());
                          }
                        }));
          }
          CompletableFuture.allOf(requests.toArray(CompletableFuture[]::new))
              .get(30, TimeUnit.SECONDS);
          engine.awaitDrained(Duration.ofSeconds(5));
          if (engine.snapshot().get().peak() > MAX_LIMIT) throw new AssertionError("Permit bound");
        }
      }
    }
    System.out.println(
        "Real-Cassandra validation passed: identical complete results for all four configurations.");
  }

  private static void study(CassandraStore store, Map<String, String> options) throws Exception {
    int seconds = integer(options, "--seconds", 20, 6, 300);
    int repeats = integer(options, "--repeats", 3, 1, 20);
    int calibration = integer(options, "--calibration-seconds", 8, 3, 60);
    long seed = Long.parseLong(options.getOrDefault("--seed", "20261028"));
    List<Integer> rates = new ArrayList<>();
    for (String value : options.getOrDefault("--rates", "20,60").split(",")) {
      int rate = Integer.parseInt(value);
      if (rate < 1 || rate > 1000) throw new IllegalArgumentException("Rates must be 1..1000");
      rates.add(rate);
    }
    Path output = Path.of(options.getOrDefault("--output", "/results/study"));
    Files.createDirectories(output);
    if (Files.exists(output.resolve("manifest.json")))
      throw new IllegalStateException("Refusing to overwrite an existing study");
    var manifest = new LinkedHashMap<String, Object>();
    manifest.put("startedAt", Instant.now().toString());
    manifest.put("status", "running");
    manifest.put("java", System.getProperty("java.runtime.version"));
    manifest.put("os", System.getProperty("os.name"));
    manifest.put("arch", System.getProperty("os.arch"));
    manifest.put("availableProcessors", Runtime.getRuntime().availableProcessors());
    manifest.put("maxHeapBytes", Runtime.getRuntime().maxMemory());
    manifest.put("cassandraVersion", store.version());
    manifest.put("driver", "com.datastax.oss:java-driver-core:4.17.0");
    manifest.put("limiter", "com.netflix.concurrency-limits:concurrency-limits-core:0.5.4");
    manifest.put(
        "adaptive",
        Map.of(
            "algorithm",
            "Gradient2Limit",
            "windowMs",
            500,
            "windowSamples",
            10,
            "longWindow",
            20,
            "min",
            4,
            "max",
            MAX_LIMIT,
            "tolerance",
            1.5,
            "smoothing",
            0.2,
            "queueSize",
            4));
    manifest.put(
        "settings",
        Map.of(
            "seconds",
            seconds,
            "repeats",
            repeats,
            "rates",
            rates,
            "seed",
            seed,
            "maxQueries",
            MAX_QUERIES,
            "maxPending",
            MAX_PENDING,
            "queryDeadlineMs",
            DEADLINE.toMillis(),
            "calibrationSeconds",
            calibration));
    manifest.put(
        "workload",
        "Regular open-loop arrivals; seeded 20% large queries; 2/200 profiles; 8 edges/profile; RF1");
    manifest.put(
        "slowdown",
        "30ms response-delivery delay after actual Cassandra completion for requests dispatched in middle third; not physical database throttling");
    manifest.put(
        "scope",
        "Single in-process service/load generator, same-host Cassandra; no RPC measurement or production-scale claim");
    writeJson(output.resolve("manifest.json"), manifest);
    validate(store);
    Map<WorkQueue.Mode, Integer> selected = new LinkedHashMap<>();
    var calibrationResults = new LinkedHashMap<String, Object>();
    Map<Integer, Integer> calibrationSuccesses = new LinkedHashMap<>();
    Map<Integer, Double> calibrationLatency = new LinkedHashMap<>();
    for (WorkQueue.Mode mode : WorkQueue.Mode.values()) {
      for (int fixed : List.of(8, 20, 64)) {
        Config config = new Config(mode, false, fixed);
        RunResult result =
            run(
                store,
                output.resolve("calibration-" + config.name()),
                config,
                calibration,
                40,
                seed + 999,
                false);
        calibrationResults.put(config.name(), result);
        calibrationSuccesses.merge(fixed, result.success(), Integer::sum);
        calibrationLatency.merge(fixed, result.smallP95() + result.largeP95(), Double::sum);
      }
    }
    int commonLimit = selectCommonLimit(calibrationSuccesses, calibrationLatency);
    for (WorkQueue.Mode mode : WorkQueue.Mode.values()) selected.put(mode, commonLimit);
    manifest.put("calibration", calibrationResults);
    manifest.put("selectedFixedLimits", selected);
    manifest.put(
        "selectionRule",
        "One common limit for both schedulers: maximize successes summed across schedulers; break ties by summed successful small/large p95. Healthy 40 qps, independent seed. Adaptive uses this same initial limit.");
    List<String> order = new ArrayList<>();
    manifest.put("runOrder", order);
    for (int repeat = 0; repeat < repeats; repeat++) {
      for (int rate : rates) {
        List<Config> configs = new ArrayList<>();
        for (WorkQueue.Mode mode : WorkQueue.Mode.values()) {
          configs.add(new Config(mode, false, selected.get(mode)));
          configs.add(new Config(mode, true, selected.get(mode)));
        }
        Collections.shuffle(configs, new Random(seed + repeat * 31L + rate));
        for (Config config : configs) {
          String name = "r" + repeat + "-qps" + rate + "-" + config.name();
          order.add(name);
          writeJson(output.resolve("manifest.json"), manifest);
          run(store, output.resolve(name), config, seconds, rate, seed + repeat, true);
        }
      }
    }
    manifest.put("status", "complete");
    manifest.put("finishedAt", Instant.now().toString());
    writeJson(output.resolve("manifest.json"), manifest);
  }

  private static RunResult run(
      Store store, Path output, Config config, int seconds, int rate, long seed, boolean slowdown)
      throws Exception {
    System.out.println("RUN " + output.getFileName());
    List<Arrival> arrivals = arrivals(seconds, rate, seed);
    // Equal warm-up reads; the measured limiter is reset after warm-up.
    for (String account : List.of("s0", "l0")) {
      store.read(true, account).toCompletableFuture().join();
      for (String profile : Dataset.profiles(account))
        store.read(false, profile).toCompletableFuture().join();
    }
    List<Double> small = Collections.synchronizedList(new ArrayList<>());
    List<Double> large = Collections.synchronizedList(new ArrayList<>());
    long origin = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100);
    RunResult result;
    try (Telemetry telemetry = new Telemetry(output);
        GraphEngine engine =
            new GraphEngine(
                store,
                config.scheduler(),
                limit(config.adaptive(), config.fixed()),
                MAX_QUERIES,
                MAX_PENDING,
                DEADLINE,
                elapsed ->
                    slowdown
                            && elapsed >= TimeUnit.SECONDS.toNanos(seconds) / 3
                            && elapsed < TimeUnit.SECONDS.toNanos(seconds) * 2 / 3
                        ? 30
                        : 0,
                telemetry,
                origin)) {
      List<CompletableFuture<Void>> pending = new ArrayList<>();
      for (Arrival arrival : arrivals) {
        long due = origin + arrival.offset();
        long remaining;
        while ((remaining = due - System.nanoTime()) > 0) LockSupport.parkNanos(remaining);
        pending.add(
            engine
                .submit(arrival.id(), arrival.account(), due)
                .thenAccept(
                    outcome -> {
                      if (outcome.status().equals("SUCCESS")) {
                        long completed = System.nanoTime();
                        if (!outcome.result().equals(Dataset.expected(arrival.account()))) {
                          throw new IllegalStateException("Incorrect result: " + arrival.account());
                        }
                        (arrival.account().startsWith("l") ? large : small)
                            .add(Telemetry.ms(completed - due));
                      }
                    }));
      }
      CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new)).get(35, TimeUnit.SECONDS);
      engine.awaitDrained(Duration.ofSeconds(35));
      var snapshot = engine.snapshot().get(2, TimeUnit.SECONDS);
      result =
          new RunResult(
              arrivals.size(),
              small.size() + large.size(),
              percentile(small, 0.95),
              percentile(large, 0.95),
              snapshot.peak());
    }
    writeJson(
        output.resolve("run.json"),
        Map.of(
            "config",
            config,
            "seconds",
            seconds,
            "rate",
            rate,
            "seed",
            seed,
            "slowdown",
            slowdown,
            "result",
            result));
    return result;
  }

  static List<Arrival> arrivals(int seconds, int rate, long seed) {
    Random random = new Random(seed);
    List<Arrival> result = new ArrayList<>();
    for (int i = 0; i < seconds * rate; i++) {
      boolean large = random.nextDouble() < 0.2;
      result.add(
          new Arrival(
              i,
              Dataset.account(large, random.nextInt(Dataset.ACCOUNTS_PER_CLASS)),
              (long) (i * 1_000_000_000.0 / rate)));
    }
    return result;
  }

  static double percentile(List<Double> values, double fraction) {
    if (values.isEmpty()) return Double.MAX_VALUE;
    var sorted = new ArrayList<>(values);
    sorted.sort(Double::compare);
    return sorted.get(Math.max(0, (int) Math.ceil(fraction * sorted.size()) - 1));
  }

  static int selectCommonLimit(Map<Integer, Integer> successes, Map<Integer, Double> latency) {
    return successes.keySet().stream()
        .min(
            (a, b) -> {
              int comparison = Integer.compare(successes.get(b), successes.get(a));
              if (comparison == 0) comparison = Double.compare(latency.get(a), latency.get(b));
              return comparison == 0 ? Integer.compare(a, b) : comparison;
            })
        .orElseThrow();
  }

  private static int integer(Map<String, String> args, String key, int fallback, int min, int max) {
    int value = Integer.parseInt(args.getOrDefault(key, Integer.toString(fallback)));
    if (value < min || value > max)
      throw new IllegalArgumentException(key + " outside allowed range");
    return value;
  }

  private static void writeJson(Path path, Object value) throws Exception {
    JSON.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), value);
  }
}
