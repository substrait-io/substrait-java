package io.substrait.extension;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** We don't depend on guava... */
class BidiMap<T1, T2> {
  private final Map<T1, T2> forwardMap;
  private final Map<T2, T1> reverseMap;

  public BidiMap(Map<T1, T2> forwardMap) {
    this.forwardMap = forwardMap;
    this.reverseMap = new HashMap<>();
  }

  public BidiMap() {
      this.forwardMap = new HashMap<>();
      this.reverseMap = new HashMap<>();
  }

  public T2 get(T1 t1) {
    return forwardMap.get(t1);
  }

  public T1 reverseGet(T2 t2) {
    return reverseMap.get(t2);
  }

  public void put(T1 t1, T2 t2) {
    forwardMap.put(t1, t2);
    reverseMap.put(t2, t1);
  }

  public Set<Map.Entry<T1, T2>> forwardEntrySet() {
      return forwardMap.entrySet();
  }

    public Set<Map.Entry<T2, T1>> reverseEntrySet() {
        return reverseMap.entrySet();
    }
}
