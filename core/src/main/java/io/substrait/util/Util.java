package io.substrait.util;

import java.util.function.Supplier;

/** Miscellaneous shared utility helpers. */
public class Util {

  /**
   * Wraps a supplier so that its value is computed at most once, on first access, and cached for
   * subsequent calls.
   *
   * @param supplier the supplier to memoize
   * @param <T> the supplied value type
   * @return a memoizing supplier delegating to {@code supplier}
   */
  public static <T> Supplier<T> memoize(Supplier<T> supplier) {
    return new Memoizer<T>(supplier);
  }

  private static class Memoizer<T> implements Supplier<T> {

    private boolean retrieved;
    private T value;
    private Supplier<T> delegate;

    public Memoizer(Supplier<T> delegate) {
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

  /** A half-open range of {@code int} values, with an inclusive start and an exclusive end. */
  public static class IntRange {
    private final int startInclusive;
    private final int endExclusive;

    /**
     * Creates a range covering {@code [startInclusive, endExclusive)}.
     *
     * @param startInclusive the inclusive lower bound
     * @param endExclusive the exclusive upper bound
     * @return the range
     */
    public static IntRange of(int startInclusive, int endExclusive) {
      return new IntRange(startInclusive, endExclusive);
    }

    private IntRange(int startInclusive, int endExclusive) {
      this.startInclusive = startInclusive;
      this.endExclusive = endExclusive;
    }

    /**
     * Returns the inclusive lower bound of the range.
     *
     * @return the inclusive start
     */
    public int getStartInclusive() {
      return startInclusive;
    }

    /**
     * Returns the exclusive upper bound of the range.
     *
     * @return the exclusive end
     */
    public int getEndExclusive() {
      return endExclusive;
    }

    /**
     * Returns whether the given value falls within this range.
     *
     * @param val the value to test
     * @return {@code true} if {@code startInclusive <= val < endExclusive}
     */
    public boolean within(int val) {
      return val >= startInclusive && val < endExclusive;
    }
  }
}
