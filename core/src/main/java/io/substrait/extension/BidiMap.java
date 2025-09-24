package io.substrait.extension;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** We don't depend on guava... */
class BidiMap<T1, T2> {
  private final Map<T1, T2> forwardMap;
  private final Map<T2, T1> reverseMap;

  BidiMap(Map<T1, T2> forwardMap) {
    this.forwardMap = forwardMap;
    this.reverseMap = new HashMap<>();
    for (Map.Entry<T1, T2> entry : forwardMap.entrySet()) {
      reverseMap.put(entry.getValue(), entry.getKey());
    }
  }

  BidiMap() {
    this.forwardMap = new HashMap<>();
    this.reverseMap = new HashMap<>();
  }

  T2 get(T1 t1) {
    return forwardMap.get(t1);
  }

  T1 reverseGet(T2 t2) {
    return reverseMap.get(t2);
  }

  /**
   * Associates the specified values in both directions. Throws if either value is already mapped to
   * a different value.
   */
  void put(T1 t1, T2 t2) {
    T2 existingForward = forwardMap.get(t1);
    T1 existingReverse = reverseMap.get(t2);

    if (existingForward != null && !existingForward.equals(t2)) {
      throw new IllegalArgumentException("Key already exists in map with different value");
    }
    if (existingReverse != null && !existingReverse.equals(t1)) {
      throw new IllegalArgumentException("Key already exists in map with different value");
    }

    forwardMap.put(t1, t2);
    reverseMap.put(t2, t1);
  }

  void merge(BidiMap<T1, T2> other) {
    for (Map.Entry<T1, T2> entry : other.forwardEntrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  Set<Map.Entry<T1, T2>> forwardEntrySet() {
    return forwardMap.entrySet();
  }

  Set<Map.Entry<T2, T1>> reverseEntrySet() {
    return reverseMap.entrySet();
  }
}
