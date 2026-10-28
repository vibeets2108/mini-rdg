package study.rdg;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

final class Telemetry implements AutoCloseable {
  private final BufferedWriter queries;
  private final BufferedWriter operations;
  private final BufferedWriter state;

  Telemetry(Path directory) throws IOException {
    Files.createDirectories(directory);
    queries = Files.newBufferedWriter(directory.resolve("queries.csv"));
    operations = Files.newBufferedWriter(directory.resolve("operations.csv"));
    state = Files.newBufferedWriter(directory.resolve("state.csv"));
    line(
        queries,
        "id,class,account,scheduled_ms,start_ms,end_ms,status,lag_ms,latency_ms,service_ms");
    line(
        operations,
        "query,root,dispatch_ms,queue_ms,driver_ms,slot_ms,inflight_at_dispatch,limit_at_dispatch,failed");
    line(state, "elapsed_ms,inflight,pending,limit,active_queries,heap_bytes");
  }

  synchronized void query(
      long id,
      String cls,
      String account,
      long scheduled,
      long start,
      long end,
      String status,
      long origin) {
    line(
        queries,
        id
            + ","
            + cls
            + ","
            + account
            + ","
            + ms(scheduled - origin)
            + ","
            + ms(start - origin)
            + ","
            + ms(end - origin)
            + ","
            + status
            + ","
            + ms(start - scheduled)
            + ","
            + ms(end - scheduled)
            + ","
            + ms(end - start));
  }

  synchronized void operation(
      long query,
      boolean root,
      long dispatched,
      long ready,
      long driverNs,
      long slotNs,
      int inflight,
      int limit,
      boolean failed,
      long origin) {
    line(
        operations,
        query
            + ","
            + root
            + ","
            + ms(dispatched - origin)
            + ","
            + ms(dispatched - ready)
            + ","
            + ms(driverNs)
            + ","
            + ms(slotNs)
            + ","
            + inflight
            + ","
            + limit
            + ","
            + failed);
  }

  synchronized void state(long now, long origin, int inflight, int pending, int limit, int active) {
    Runtime r = Runtime.getRuntime();
    line(
        state,
        ms(now - origin)
            + ","
            + inflight
            + ","
            + pending
            + ","
            + limit
            + ","
            + active
            + ","
            + (r.totalMemory() - r.freeMemory()));
  }

  static double ms(long nanos) {
    return nanos / 1_000_000.0;
  }

  private static void line(BufferedWriter writer, String line) {
    try {
      writer.write(line);
      writer.newLine();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    queries.close();
    operations.close();
    state.close();
  }
}
