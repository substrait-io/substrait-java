package io.substrait.expression;

import org.immutables.value.Value;

/**
 * A bound of a window frame, such as a number of rows preceding/following, current row or
 * unbounded.
 */
@Value.Enclosing
public interface WindowBound {

  /** Shared instance representing the current row bound. */
  CurrentRow CURRENT_ROW = ImmutableWindowBound.CurrentRow.builder().build();

  /** Shared instance representing an unbounded frame bound. */
  Unbounded UNBOUNDED = ImmutableWindowBound.Unbounded.builder().build();

  /**
   * Visitor over the concrete {@link WindowBound} kinds.
   *
   * @param <R> the return type
   * @param <E> the exception type that may be thrown
   */
  interface WindowBoundVisitor<R, E extends Throwable> {
    /**
     * Visits a {@link Preceding} bound.
     *
     * @param preceding the preceding bound
     * @return the result of the visit
     */
    R visit(Preceding preceding);

    /**
     * Visits a {@link Following} bound.
     *
     * @param following the following bound
     * @return the result of the visit
     */
    R visit(Following following);

    /**
     * Visits a {@link CurrentRow} bound.
     *
     * @param currentRow the current-row bound
     * @return the result of the visit
     */
    R visit(CurrentRow currentRow);

    /**
     * Visits an {@link Unbounded} bound.
     *
     * @param unbounded the unbounded bound
     * @return the result of the visit
     */
    R visit(Unbounded unbounded);
  }

  /**
   * Accepts a visitor for this window bound.
   *
   * @param <R> the return type
   * @param <E> the exception type that may be thrown
   * @param visitor the visitor
   * @return the result of the visit
   */
  <R, E extends Throwable> R accept(WindowBoundVisitor<R, E> visitor);

  /** A bound a fixed number of rows before the current row. */
  @Value.Immutable
  abstract class Preceding implements WindowBound {
    /**
     * Returns the number of rows preceding the current row.
     *
     * @return the offset
     */
    public abstract long offset();

    /**
     * Creates a {@link Preceding} bound with the given offset.
     *
     * @param offset the number of rows preceding the current row
     * @return the preceding bound
     */
    public static Preceding of(long offset) {
      return ImmutableWindowBound.Preceding.builder().offset(offset).build();
    }

    @Override
    public <R, E extends Throwable> R accept(WindowBoundVisitor<R, E> visitor) {
      return visitor.visit(this);
    }
  }

  /** A bound a fixed number of rows after the current row. */
  @Value.Immutable
  abstract class Following implements WindowBound {
    /**
     * Returns the number of rows following the current row.
     *
     * @return the offset
     */
    public abstract long offset();

    /**
     * Creates a {@link Following} bound with the given offset.
     *
     * @param offset the number of rows following the current row
     * @return the following bound
     */
    public static Following of(long offset) {
      return ImmutableWindowBound.Following.builder().offset(offset).build();
    }

    @Override
    public <R, E extends Throwable> R accept(WindowBoundVisitor<R, E> visitor) {
      return visitor.visit(this);
    }
  }

  /** The bound at the current row. */
  @Value.Immutable
  abstract class CurrentRow implements WindowBound {
    @Override
    public <R, E extends Throwable> R accept(WindowBoundVisitor<R, E> visitor) {
      return visitor.visit(this);
    }
  }

  /** An unbounded frame bound (the start or end of the partition). */
  @Value.Immutable
  abstract class Unbounded implements WindowBound {
    @Override
    public <R, E extends Throwable> R accept(WindowBoundVisitor<R, E> visitor) {
      return visitor.visit(this);
    }
  }
}
