package study.rdg;

import java.util.List;
import java.util.concurrent.CompletionStage;

interface Store {
  CompletionStage<List<String>> read(boolean root, String id);
}
