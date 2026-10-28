package study.rdg;

import static org.junit.jupiter.api.Assertions.*;

import com.netflix.concurrency.limits.limit.FixedLimit;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class GraphEngineTest {
  private record Read(boolean root, String id, CompletableFuture<List<String>> response) {}

  private static final class ManualStore implements Store {
    final LinkedBlockingQueue<Read> reads = new LinkedBlockingQueue<>();

    public CompletableFuture<List<String>> read(boolean root, String id) {
      var future = new CompletableFuture<List<String>>();
      reads.add(new Read(root, id, future));
      return future;
    }

    Read next() throws Exception {
      Read read = reads.poll(2, TimeUnit.SECONDS);
      assertNotNull(read, "Expected storage dispatch");
      return read;
    }
  }

  private GraphEngine engine(Store store, int limit, int admitted, int queued, Duration deadline) {
    return new GraphEngine(
        store,
        WorkQueue.Mode.FIFO,
        FixedLimit.of(limit),
        admitted,
        queued,
        deadline,
        ignored -> 0,
        null,
        System.nanoTime());
  }

  @Test
  void allFourConfigurationsReturnIdenticalGraphs() throws Exception {
    Store store =
        (root, id) ->
            CompletableFuture.completedFuture(root ? Dataset.profiles(id) : Dataset.titles(id));
    for (var mode : WorkQueue.Mode.values()) {
      for (boolean adaptive : List.of(false, true)) {
        try (var engine =
            new GraphEngine(
                store,
                mode,
                Main.limit(adaptive, 20),
                32,
                5000,
                Duration.ofSeconds(10),
                ignored -> 0,
                null,
                System.nanoTime())) {
          var requests = new ArrayList<CompletableFuture<GraphEngine.Outcome>>();
          for (int i = 0; i < 12; i++)
            requests.add(engine.submit(i, i % 2 == 0 ? "l0" : "s0", System.nanoTime()));
          for (int i = 0; i < requests.size(); i++) {
            var result = requests.get(i).get(10, TimeUnit.SECONDS);
            assertEquals("SUCCESS", result.status());
            assertEquals(Dataset.expected(i % 2 == 0 ? "l0" : "s0"), result.result());
          }
          engine.awaitDrained(Duration.ofSeconds(2));
          assertTrue(engine.snapshot().get().peak() <= 64);
        }
      }
    }
  }

  @Test
  void timeoutDoesNotReleaseOutstandingStoragePermitOrAdmission() throws Exception {
    var store = new ManualStore();
    try (var engine = engine(store, 1, 1, 100, Duration.ofMillis(100))) {
      var first = engine.submit(1, "s0", System.nanoTime());
      Read read = store.next();
      assertEquals("TIMEOUT", first.get(2, TimeUnit.SECONDS).status());
      assertEquals(1, engine.snapshot().get().inflight());
      assertEquals("REJECTED", engine.submit(2, "s0", System.nanoTime()).get().status());
      read.response().complete(List.of());
      engine.awaitDrained(Duration.ofSeconds(2));
      assertEquals(0, engine.snapshot().get().inflight());
    }
  }

  @Test
  void cancellationDropsPendingWorkButDrainsActualIo() throws Exception {
    var store = new ManualStore();
    try (var engine = engine(store, 1, 5, 100, Duration.ofSeconds(5))) {
      var future = engine.submit(1, "s0", System.nanoTime());
      store.next().response().complete(List.of("p1", "p2"));
      Read running = store.next();
      future.cancel(false);
      engine.snapshot().get();
      assertEquals(1, engine.snapshot().get().inflight());
      assertEquals(0, engine.snapshot().get().pending());
      running.response().complete(List.of("t"));
      engine.awaitDrained(Duration.ofSeconds(2));
      assertTrue(store.reads.isEmpty());
    }
  }

  @Test
  void requiredStorageFailureCannotReturnSuccessOrPartialGraph() throws Exception {
    var store = new ManualStore();
    try (var engine = engine(store, 2, 5, 100, Duration.ofSeconds(5))) {
      var future = engine.submit(1, "s0", System.nanoTime());
      store.next().response().complete(List.of("p1", "p2"));
      Read a = store.next();
      Read b = store.next();
      a.response().completeExceptionally(new IllegalStateException("injected"));
      var result = future.get(2, TimeUnit.SECONDS);
      assertEquals("STORAGE_ERROR", result.status());
      assertTrue(result.result().isEmpty());
      assertEquals(1, engine.snapshot().get().inflight());
      b.response().complete(List.of("t"));
      engine.awaitDrained(Duration.ofSeconds(2));
    }
  }

  @Test
  void pendingBudgetRejectsWideExpansionBeforeEnqueue() throws Exception {
    Store store = (root, id) -> CompletableFuture.completedFuture(List.of("a", "b", "c"));
    try (var engine = engine(store, 1, 2, 2, Duration.ofSeconds(5))) {
      assertEquals(
          "QUEUE_FULL",
          engine.submit(1, "l0", System.nanoTime()).get(2, TimeUnit.SECONDS).status());
      engine.awaitDrained(Duration.ofSeconds(2));
      assertEquals(0, engine.snapshot().get().pending());
    }
  }

  @Test
  void admissionAndInflightCapsHold() throws Exception {
    var store = new ManualStore();
    try (var engine = engine(store, 2, 3, 100, Duration.ofSeconds(5))) {
      var a = engine.submit(1, "s0", System.nanoTime());
      var b = engine.submit(2, "s0", System.nanoTime());
      var c = engine.submit(3, "s0", System.nanoTime());
      assertEquals("REJECTED", engine.submit(4, "s0", System.nanoTime()).get().status());
      Read r1 = store.next(), r2 = store.next();
      assertEquals(2, engine.snapshot().get().inflight());
      assertTrue(store.reads.isEmpty());
      r1.response().complete(List.of());
      Read r3 = store.next();
      r2.response().complete(List.of());
      r3.response().complete(List.of());
      CompletableFuture.allOf(a, b, c).get(2, TimeUnit.SECONDS);
      assertEquals(2, engine.snapshot().get().peak());
    }
  }

  @Test
  void synchronousStoreExceptionIsAnObservableFailure() throws Exception {
    Store store =
        (root, id) -> {
          throw new IllegalArgumentException("injected");
        };
    try (var engine = engine(store, 1, 1, 10, Duration.ofSeconds(5))) {
      assertEquals(
          "STORAGE_ERROR",
          engine.submit(1, "s0", System.nanoTime()).get(2, TimeUnit.SECONDS).status());
      engine.awaitDrained(Duration.ofSeconds(2));
    }
  }

  @Test
  void workloadAndPercentilesAreDeterministic() {
    assertEquals(Main.arrivals(2, 20, 7), Main.arrivals(2, 20, 7));
    assertNotEquals(Main.arrivals(2, 20, 7), Main.arrivals(2, 20, 8));
    assertEquals(3.0, Main.percentile(List.of(3.0, 1.0, 2.0), 0.95));
  }
}
