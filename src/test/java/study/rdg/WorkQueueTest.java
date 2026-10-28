package study.rdg;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class WorkQueueTest {
  @Test
  void fifoPreservesGlobalInsertionOrder() {
    var q = new WorkQueue<String>(WorkQueue.Mode.FIFO);
    q.add(1, "a");
    q.add(1, "b");
    q.add(2, "c");
    q.add(1, "d");
    assertEquals(List.of("a", "b", "c", "d"), drain(q));
  }

  @Test
  void roundRobinTakesOnePendingOperationPerQueryPerTurn() {
    var q = new WorkQueue<String>(WorkQueue.Mode.ROUND_ROBIN);
    q.add(1, "a");
    q.add(1, "b");
    q.add(1, "d");
    q.add(2, "c");
    assertEquals(List.of("a", "c", "b", "d"), drain(q));
  }

  @Test
  void cancellationRemovesAllPendingWorkAndDoesNotLoseOtherQueries() {
    for (var mode : WorkQueue.Mode.values()) {
      var q = new WorkQueue<String>(mode);
      q.add(1, "a");
      q.add(2, "b");
      q.add(1, "c");
      q.remove(1);
      assertEquals(1, q.size());
      assertEquals(List.of("b"), drain(q));
      q.add(1, "new");
      assertEquals("new", q.poll().value());
      assertNull(q.poll());
    }
  }

  private List<String> drain(WorkQueue<String> q) {
    List<String> values = new ArrayList<>();
    while (q.size() > 0) values.add(q.poll().value());
    assertNull(q.poll());
    return values;
  }
}
