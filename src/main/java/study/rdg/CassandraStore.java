package study.rdg;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

final class CassandraStore implements Store, AutoCloseable {
  private final CqlSession session;
  private final PreparedStatement roots;
  private final PreparedStatement leaves;

  CassandraStore(String host) {
    var config =
        DriverConfigLoader.programmaticBuilder()
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
            .withString(DefaultDriverOption.RETRY_POLICY_CLASS, "study.rdg.NoRetries")
            .withString(
                DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS,
                "com.datastax.oss.driver.internal.core.specex.NoSpeculativeExecutionPolicy")
            .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 512)
            .withInt(DefaultDriverOption.NETTY_IO_SIZE, 2)
            .withInt(DefaultDriverOption.NETTY_ADMIN_SIZE, 1)
            .build();
    session =
        CqlSession.builder()
            .addContactPoint(new InetSocketAddress(host, 9042))
            .withLocalDatacenter("datacenter1")
            .withConfigLoader(config)
            .build();
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS rdg_study WITH replication = "
            + "{'class':'SimpleStrategy','replication_factor':1}");
    for (String table : List.of("profiles", "watched")) {
      session.execute(
          "CREATE TABLE IF NOT EXISTS rdg_study."
              + table
              + " (source text, target text, PRIMARY KEY (source, target))");
    }
    roots = session.prepare("SELECT target FROM rdg_study.profiles WHERE source=?");
    leaves = session.prepare("SELECT target FROM rdg_study.watched WHERE source=?");
  }

  void seed() {
    var root = session.prepare("INSERT INTO rdg_study.profiles (source,target) VALUES (?,?)");
    var leaf = session.prepare("INSERT INTO rdg_study.watched (source,target) VALUES (?,?)");
    List<CompletableFuture<?>> writes = new ArrayList<>();
    for (boolean large : List.of(false, true)) {
      for (int i = 0; i < Dataset.ACCOUNTS_PER_CLASS; i++) {
        String a = Dataset.account(large, i);
        for (String p : Dataset.profiles(a)) {
          writes.add(session.executeAsync(root.bind(a, p)).toCompletableFuture());
          for (String t : Dataset.titles(p)) {
            writes.add(session.executeAsync(leaf.bind(p, t)).toCompletableFuture());
          }
          if (writes.size() >= 128) {
            CompletableFuture.allOf(writes.toArray(CompletableFuture[]::new)).join();
            writes.clear();
          }
        }
      }
    }
    CompletableFuture.allOf(writes.toArray(CompletableFuture[]::new)).join();
  }

  @Override
  public CompletionStage<List<String>> read(boolean root, String id) {
    return session
        .executeAsync((root ? roots : leaves).bind(id))
        .thenApply(
            rows -> {
              if (rows.hasMorePages()) {
                throw new IllegalStateException("Dataset exceeds the single-page study contract");
              }
              List<String> result = new ArrayList<>();
              rows.currentPage().forEach(row -> result.add(row.getString("target")));
              return result;
            });
  }

  String version() {
    return session.execute("SELECT release_version FROM system.local").one().getString(0);
  }

  @Override
  public void close() {
    session.close();
  }
}
