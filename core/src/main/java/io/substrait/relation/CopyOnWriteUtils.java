package io.substrait.relation;

import io.substrait.util.VisitationContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/** Provides common utilities for copy-on-write visitations */
public class CopyOnWriteUtils {

  public static boolean allEmpty(Optional<?>... optionals) {
    return Arrays.stream(optionals).noneMatch(Optional::isPresent);
  }

  /** The `or` method on Optional instances is a Java 9+ feature */
  public static <T> Optional<T> or(Optional<T> left, Supplier<? extends Optional<T>> right) {
    if (left.isPresent()) {
      return left;
    } else {
      return right.get();
    }
  }

  @FunctionalInterface
  public interface TransformFunction<T, C extends VisitationContext, E extends Exception> {
    Optional<T> apply(T t, C context) throws E;
  }

  /**
   * Applies the given transformation function to each item in the list. If any of the list items
   * are transformed, returns a new list in which each item is either
   *
   * <ul>
   *   <li>a transformed new item replacing an old item
   *   <li>the original item in the position it was in
   * </ul>
   *
   * @param items the list of items to transform
   * @param transform the transformation function to apply to each item
   * @return An empty optional if none of the items have changed. An optional containing a new list
   *     otherwise.
   */
  public static <I, C extends VisitationContext, E extends Exception>
      Optional<List<I>> transformList(
          List<I> items, C context, TransformFunction<I, C, E> transform) throws E {
    List<I> newItems = new ArrayList<>();
    boolean listUpdated = false;
    for (I item : items) {
      Optional<I> newItem = transform.apply(item, context);
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
