package io.substrait.util;

import java.util.function.Supplier;

public class Util {

  public static <T> Supplier<T> memoize(final Supplier<T> supplier) {
    return new Memoizer<T>(supplier);
  }

  private static class Memoizer<T> implements Supplier<T> {

    private boolean retrieved;
    private T value;
    private final Supplier<T> delegate;

    public Memoizer(final Supplier<T> delegate) {
      this.delegate = delegate;
    }

    @Override
    public T get() {
      if (!retrieved) {
        value = delegate.get();
        retrieved = true;
      }
      return value;
    }
  }

  public static class IntRange {
    private final int startInclusive;
    private final int endExclusive;

    public static IntRange of(final int startInclusive, final int endExclusive) {
      return new IntRange(startInclusive, endExclusive);
    }

    private IntRange(final int startInclusive, final int endExclusive) {
      this.startInclusive = startInclusive;
      this.endExclusive = endExclusive;
    }

    public int getStartInclusive() {
      return startInclusive;
    }

    public int getEndExclusive() {
      return endExclusive;
    }

    public boolean within(final int val) {
      return val >= startInclusive && val < endExclusive;
    }
  }
}
