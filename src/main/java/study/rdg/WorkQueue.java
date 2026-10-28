package study.rdg;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;

final class WorkQueue<T> {
  enum Mode {
    FIFO,
    ROUND_ROBIN
  }

  record Item<T>(long query, T value) {}

  private final Mode mode;
  private final Deque<Item<T>> fifo = new ArrayDeque<>();
  private final LinkedHashMap<Long, Deque<T>> groups = new LinkedHashMap<>();
  private int size;

  WorkQueue(Mode mode) {
    this.mode = mode;
  }

  void add(long query, T value) {
    if (mode == Mode.FIFO) fifo.addLast(new Item<>(query, value));
    else groups.computeIfAbsent(query, ignored -> new ArrayDeque<>()).addLast(value);
    size++;
  }

  Item<T> poll() {
    if (size == 0) return null;
    size--;
    if (mode == Mode.FIFO) return fifo.removeFirst();
    var entry = groups.entrySet().iterator().next();
    long id = entry.getKey();
    Deque<T> values = entry.getValue();
    T value = values.removeFirst();
    groups.remove(id);
    if (!values.isEmpty()) groups.put(id, values);
    return new Item<>(id, value);
  }

  void remove(long query) {
    if (mode == Mode.FIFO) {
      int before = fifo.size();
      fifo.removeIf(item -> item.query() == query);
      size -= before - fifo.size();
    } else {
      Deque<T> removed = groups.remove(query);
      if (removed != null) size -= removed.size();
    }
  }

  int size() {
    return size;
  }
}
