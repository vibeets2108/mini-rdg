package study.rdg;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;

/** Keeps one logical read from silently producing additional attempts. */
public final class NoRetries implements RetryPolicy {
  public NoRetries(DriverContext context, String profile) {}

  @Override
  public RetryDecision onReadTimeout(
      Request request,
      ConsistencyLevel consistency,
      int required,
      int received,
      boolean dataPresent,
      int retryCount) {
    return RetryDecision.RETHROW;
  }

  @Override
  public RetryDecision onWriteTimeout(
      Request request,
      ConsistencyLevel consistency,
      WriteType type,
      int required,
      int received,
      int retryCount) {
    return RetryDecision.RETHROW;
  }

  @Override
  public RetryDecision onUnavailable(
      Request request, ConsistencyLevel consistency, int required, int alive, int retryCount) {
    return RetryDecision.RETHROW;
  }

  @Override
  public RetryDecision onRequestAborted(Request request, Throwable error, int retryCount) {
    return RetryDecision.RETHROW;
  }

  @Override
  public RetryDecision onErrorResponse(
      Request request, CoordinatorException error, int retryCount) {
    return RetryDecision.RETHROW;
  }

  @Override
  public void close() {}
}
