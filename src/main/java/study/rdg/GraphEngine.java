package study.rdg;

import com.netflix.concurrency.limits.Limit;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.LongUnaryOperator;

final class GraphEngine implements AutoCloseable {
  record Outcome(String status, Map<String, List<String>> result) {}

  record Snapshot(int inflight, int pending, int active, int peak) {}

  private record Work(Query query, boolean root, String key, long ready) {}

  private static final class Query {
    final long id, scheduled, start, deadline;
    final String account;
    final CompletableFuture<Outcome> future = new CompletableFuture<>();
    final Map<String, List<String>> result = new TreeMap<>();
    int remaining;
    int inflight;
    boolean done;
    ScheduledFuture<?> timer;

    Query(long id, String account, long scheduled, long start, Duration timeout) {
      this.id = id;
      this.account = account;
      this.scheduled = scheduled;
      this.start = start;
      this.deadline = start + timeout.toNanos();
    }
  }

  private final ScheduledThreadPoolExecutor loop = new ScheduledThreadPoolExecutor(1);
  private final Store store;
  private final WorkQueue<Work> queue;
  private final Limit limit;
  private final Semaphore admission;
  private final int maxPending;
  private final Duration timeout;
  private final LongUnaryOperator delayMillis;
  private final Map<Long, Query> active = new HashMap<>();
  private final Telemetry telemetry;
  private final long origin;
  private int inflight;
  private int peak;
  private volatile boolean closed;

  GraphEngine(
      Store store,
      WorkQueue.Mode mode,
      Limit limit,
      int maxQueries,
      int maxPending,
      Duration timeout,
      LongUnaryOperator delayMillis,
      Telemetry telemetry,
      long origin) {
    this.store = store;
    this.queue = new WorkQueue<>(mode);
    this.limit = limit;
    this.admission = new Semaphore(maxQueries);
    this.maxPending = maxPending;
    this.timeout = timeout;
    this.delayMillis = delayMillis;
    this.telemetry = telemetry;
    this.origin = origin;
    loop.setRemoveOnCancelPolicy(true);
    loop.scheduleAtFixedRate(
        () -> {
          if (telemetry != null)
            telemetry.state(
                System.nanoTime(), origin, inflight, queue.size(), limit.getLimit(), active.size());
        },
        0,
        100,
        TimeUnit.MILLISECONDS);
  }

  CompletableFuture<Outcome> submit(long id, String account, long scheduled) {
    long start = System.nanoTime();
    if (closed) throw new IllegalStateException("Engine closed");
    Query q = new Query(id, account, scheduled, start, timeout);
    if (!admission.tryAcquire()) {
      log(q, "REJECTED");
      return CompletableFuture.completedFuture(new Outcome("REJECTED", Map.of()));
    }
    loop.execute(
        () -> {
          active.put(id, q);
          q.timer =
              loop.schedule(
                  () -> {
                    finish(q, "TIMEOUT");
                    pump();
                  },
                  Math.max(0, q.deadline - System.nanoTime()),
                  TimeUnit.NANOSECONDS);
          if (queue.size() >= maxPending) finish(q, "QUEUE_FULL");
          else queue.add(q.id, new Work(q, true, account, System.nanoTime()));
          pump();
        });
    q.future.whenComplete(
        (v, e) -> {
          if (q.future.isCancelled())
            loop.execute(
                () -> {
                  finish(q, "CANCELLED");
                  pump();
                });
        });
    return q.future;
  }

  private void pump() {
    while (inflight < limit.getLimit() && queue.size() > 0) {
      Work work = queue.poll().value();
      Query q = work.query();
      if (q.done) continue;
      if (System.nanoTime() >= q.deadline) {
        finish(q, "TIMEOUT");
        continue;
      }
      long dispatch = System.nanoTime();
      int dispatchLimit = limit.getLimit();
      int dispatchedInFlight = ++inflight;
      peak = Math.max(peak, inflight);
      q.inflight++;
      long extraDelay = delayMillis.applyAsLong(dispatch - origin);
      try {
        store
            .read(work.root(), work.key())
            .whenComplete(
                (rows, error) -> {
                  long driverNs = System.nanoTime() - dispatch;
                  loop.schedule(
                      () ->
                          completed(
                              work,
                              rows,
                              error,
                              dispatch,
                              driverNs,
                              dispatchedInFlight,
                              dispatchLimit),
                      extraDelay,
                      TimeUnit.MILLISECONDS);
                });
      } catch (RuntimeException error) {
        loop.execute(
            () ->
                completed(
                    work,
                    null,
                    error,
                    dispatch,
                    System.nanoTime() - dispatch,
                    dispatchedInFlight,
                    dispatchLimit));
      }
    }
  }

  private void completed(
      Work work,
      List<String> rows,
      Throwable error,
      long dispatch,
      long driverNs,
      int dispatchInflight,
      int dispatchLimit) {
    Query q = work.query();
    long slotNs = System.nanoTime() - dispatch;
    inflight--;
    q.inflight--;
    limit.onSample(dispatch, Math.max(1, slotNs), dispatchInflight, error != null);
    if (telemetry != null)
      telemetry.operation(
          q.id,
          work.root(),
          dispatch,
          work.ready(),
          driverNs,
          slotNs,
          dispatchInflight,
          dispatchLimit,
          error != null,
          origin);
    if (!q.done) {
      if (System.nanoTime() >= q.deadline) finish(q, "TIMEOUT");
      else if (error != null) finish(q, "STORAGE_ERROR");
      else if (work.root()) {
        if (queue.size() + rows.size() > maxPending) finish(q, "QUEUE_FULL");
        else {
          q.remaining = rows.size();
          long ready = System.nanoTime();
          for (String profile : rows) queue.add(q.id, new Work(q, false, profile, ready));
          if (rows.isEmpty()) finish(q, "SUCCESS");
        }
      } else {
        q.result.put(work.key(), List.copyOf(rows));
        if (--q.remaining == 0) finish(q, "SUCCESS");
      }
    }
    releaseIfDrained(q);
    pump();
  }

  private void finish(Query q, String status) {
    if (q.done) return;
    q.done = true;
    queue.remove(q.id);
    if (q.timer != null) q.timer.cancel(false);
    log(q, status);
    q.future.complete(
        new Outcome(status, status.equals("SUCCESS") ? Map.copyOf(q.result) : Map.of()));
    releaseIfDrained(q);
  }

  private void releaseIfDrained(Query q) {
    if (q.done && q.inflight == 0 && active.remove(q.id) != null) admission.release();
  }

  private void log(Query q, String status) {
    if (telemetry != null)
      telemetry.query(
          q.id,
          q.account.startsWith("l") ? "large" : "small",
          q.account,
          q.scheduled,
          q.start,
          System.nanoTime(),
          status,
          origin);
  }

  CompletableFuture<Snapshot> snapshot() {
    CompletableFuture<Snapshot> result = new CompletableFuture<>();
    loop.execute(() -> result.complete(new Snapshot(inflight, queue.size(), active.size(), peak)));
    return result;
  }

  void awaitDrained(Duration budget) throws Exception {
    long end = System.nanoTime() + budget.toNanos();
    do {
      Snapshot s = snapshot().get(2, TimeUnit.SECONDS);
      if (s.active() == 0 && s.inflight() == 0 && s.pending() == 0) return;
      Thread.sleep(10);
    } while (System.nanoTime() < end);
    throw new IllegalStateException("Storage did not drain; refusing to hide outstanding work");
  }

  @Override
  public void close() throws Exception {
    closed = true;
    try {
      awaitDrained(Duration.ofSeconds(40));
    } finally {
      loop.shutdown();
      if (!loop.awaitTermination(5, TimeUnit.SECONDS)) loop.shutdownNow();
    }
  }
}
