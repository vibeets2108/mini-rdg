package study.rdg;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastax.oss.driver.api.core.retry.RetryDecision;
import org.junit.jupiter.api.Test;

class NoRetriesTest {
  @Test
  void everyFailurePropagatesWithoutHiddenAttempts() {
    try (var policy = new NoRetries(null, "default")) {
      assertEquals(RetryDecision.RETHROW, policy.onReadTimeout(null, null, 2, 1, false, 0));
      assertEquals(RetryDecision.RETHROW, policy.onWriteTimeout(null, null, null, 2, 1, 0));
      assertEquals(RetryDecision.RETHROW, policy.onUnavailable(null, null, 2, 1, 0));
      assertEquals(RetryDecision.RETHROW, policy.onRequestAborted(null, new RuntimeException(), 0));
      assertEquals(RetryDecision.RETHROW, policy.onErrorResponse(null, null, 0));
    }
  }
}
