package io.substrait.relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/** Base class for Copy On Write visitors. Provides common utils. */
public abstract class CopyOnWriteVisitor<T, E extends Exception> {

  protected interface TransformFunction<T, E extends Exception> {
    Optional<T> apply(T t) throws E;
  }

  protected boolean allEmpty(Optional<?>... optionals) {
    return Arrays.stream(optionals).noneMatch(Optional::isPresent);
  }

  /** The `or` method on Optional instances is a Java 9+ feature */
  protected static <T> Optional<T> or(Optional<T> left, Supplier<? extends Optional<T>> right) {
    if (left.isPresent()) {
      return left;
    } else {
      return right.get();
    }
  }

  /**
   * Applies the given transformation function to each item in the list. If any of the list items
   * are transformed, returns a new list in which each item is either
   * <ul>
   *     <li>a transformed new item replacing an old item</li>
   *     <li>the original item in the position it was in</li>
   * </ul>
   *
   * @param items the list of items to transform
   * @param transform the transformation function to apply to each item
   * @return An empty optional if none of the items have changed. An optional containing a new list
   *     otherwise.
   */
  protected <ITEM> Optional<List<ITEM>> transformList(
      List<ITEM> items, TransformFunction<ITEM, E> transform) throws E {
    ArrayList<ITEM> newItems = new ArrayList<>();
    boolean listUpdated = false;
    for (ITEM item : items) {
      Optional<ITEM> newItem = transform.apply(item);
      if (newItem.isPresent()) {
        newItems.add(newItem.get());
        listUpdated = true;
      } else {
        newItems.add(item);
      }
    }
    return listUpdated ? Optional.of(newItems) : Optional.empty();
  }
}
