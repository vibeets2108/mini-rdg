package study.rdg;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

final class Dataset {
  static final int ACCOUNTS_PER_CLASS = 16;
  static final int SMALL = 2;
  static final int LARGE = 200;
  static final int TITLES = 8;

  static String account(boolean large, int index) {
    return (large ? "l" : "s") + index;
  }

  static List<String> profiles(String account) {
    int count = account.startsWith("l") ? LARGE : SMALL;
    List<String> result = new ArrayList<>();
    for (int i = 0; i < count; i++) result.add(account + "-p" + i);
    result.sort(String::compareTo);
    return result;
  }

  static List<String> titles(String profile) {
    List<String> result = new ArrayList<>();
    for (int i = 0; i < TITLES; i++) result.add(profile + "-t" + i);
    return result;
  }

  static Map<String, List<String>> expected(String account) {
    Map<String, List<String>> result = new TreeMap<>();
    for (String p : profiles(account)) result.put(p, titles(p));
    return result;
  }
}
