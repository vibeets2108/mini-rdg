package study.rdg;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.concurrency.limits.Limit;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class RuntimeTest {
  @Test
  void commonCalibrationPrefersSuccessThenLatencyThenLowerLimit() {
    assertEquals(
        20,
        Main.selectCommonLimit(Map.of(8, 90, 20, 100, 64, 100), Map.of(8, 1.0, 20, 3.0, 64, 4.0)));
    assertEquals(8, Main.selectCommonLimit(Map.of(8, 100, 20, 100), Map.of(8, 3.0, 20, 3.0)));
  }

  @Test
  void manifestJsonRoundTripsWithTheResolvedDependencyVersions() throws Exception {
    var mapper = new ObjectMapper();
    String json = mapper.writeValueAsString(Map.of("status", "complete", "rates", List.of(20, 60)));
    assertEquals("complete", mapper.readTree(json).get("status").asText());
  }

  @Test
  void shrinkDoesNotDispatchUntilExistingWorkFallsBelowNewLimit() throws Exception {
    class MutableLimit implements Limit {
      volatile int value = 2;

      public int getLimit() {
        return value;
      }

      public void notifyOnChange(Consumer<Integer> consumer) {}

      public void onSample(long start, long rtt, int inflight, boolean drop) {}
    }
    var limit = new MutableLimit();
    var reads = new LinkedBlockingQueue<CompletableFuture<List<String>>>();
    Store store =
        (root, id) -> {
          var future = new CompletableFuture<List<String>>();
          reads.add(future);
          return future;
        };
    try (var engine =
        new GraphEngine(
            store,
            WorkQueue.Mode.ROUND_ROBIN,
            limit,
            10,
            100,
            Duration.ofSeconds(5),
            ignored -> 0,
            null,
            System.nanoTime())) {
      var a = engine.submit(1, "s0", System.nanoTime());
      var b = engine.submit(2, "s0", System.nanoTime());
      var c = engine.submit(3, "s0", System.nanoTime());
      var first = reads.poll(2, TimeUnit.SECONDS);
      var second = reads.poll(2, TimeUnit.SECONDS);
      assertNotNull(first);
      assertNotNull(second);
      limit.value = 1;
      first.complete(List.of());
      engine.snapshot().get(2, TimeUnit.SECONDS);
      assertEquals(1, engine.snapshot().get().inflight());
      assertTrue(reads.isEmpty());
      second.complete(List.of());
      var third = reads.poll(2, TimeUnit.SECONDS);
      assertNotNull(third);
      third.complete(List.of());
      CompletableFuture.allOf(a, b, c).get(2, TimeUnit.SECONDS);
    }
  }
}
