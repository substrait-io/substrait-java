package io.substrait.expression;

import java.util.List;
import java.util.Optional;
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
   * Returns the offset as a strictly positive {@code int64} literal, if it is representable as one.
   * Mirrors the spec's rule that the legacy {@code offset} field can only carry the int64-literal
   * equivalent of {@code offset_expr}.
   *
   * @param offset the offset expression of a {@link Preceding} or {@link Following} bound
   * @return the literal value, or empty if the expression is not a plain, strictly positive {@code
   *     i64} literal
   */
  static Optional<Long> toLiteralOffset(Expression offset) {
    long value;
    if (offset instanceof Expression.I64Literal) {
      value = ((Expression.I64Literal) offset).value();
    } else if (offset instanceof Expression.I32Literal) {
      value = ((Expression.I32Literal) offset).value();
    } else if (offset instanceof Expression.I16Literal) {
      value = ((Expression.I16Literal) offset).value();
    } else if (offset instanceof Expression.I8Literal) {
      value = ((Expression.I8Literal) offset).value();
    } else {
      return Optional.empty();
    }
    return value > 0 ? Optional.of(value) : Optional.empty();
  }

  /**
   * Validates a window's bounds type against its bounds, per the spec's rule that {@code
   * bounds_type} is required whenever either bound is {@link CurrentRow}, {@link Preceding}, or
   * {@link Following} (i.e. whenever a bound is not {@link Unbounded}).
   *
   * @param boundsType the window's bounds type
   * @param lowerBound the window's lower bound
   * @param upperBound the window's upper bound
   * @throws IllegalArgumentException if {@code boundsType} is {@code UNSPECIFIED} despite a bound
   *     that requires one
   */
  static void checkBoundsType(
      Expression.WindowBoundsType boundsType, WindowBound lowerBound, WindowBound upperBound) {
    if (boundsType == Expression.WindowBoundsType.UNSPECIFIED
        && (!(lowerBound instanceof Unbounded) || !(upperBound instanceof Unbounded))) {
      throw new IllegalArgumentException(
          "bounds_type is required when either window bound is CurrentRow, Preceding, or"
              + " Following, but was BOUNDS_TYPE_UNSPECIFIED");
    }
  }

  /**
   * Validates a RANGE window's ordering against its bounds, per the spec's rule that a RANGE frame
   * with a {@link Preceding} or {@link Following} bound must have exactly one ordering expression,
   * which must not use {@code SORT_DIRECTION_CLUSTERED}.
   *
   * @param boundsType the window's bounds type
   * @param lowerBound the window's lower bound
   * @param upperBound the window's upper bound
   * @param sorts the window's ordering expressions
   * @param function identifies the window function being validated, for the exception message
   * @throws IllegalArgumentException if {@code boundsType} is {@code RANGE} and either bound is
   *     {@link Preceding} or {@link Following}, and {@code sorts} does not hold exactly one
   *     ordering expression whose direction is not {@code SORT_DIRECTION_CLUSTERED}
   */
  static void checkRangeOrdering(
      Expression.WindowBoundsType boundsType,
      WindowBound lowerBound,
      WindowBound upperBound,
      List<Expression.SortField> sorts,
      String function) {
    boolean needsSingleOrdering =
        boundsType == Expression.WindowBoundsType.RANGE
            && (lowerBound instanceof Preceding
                || lowerBound instanceof Following
                || upperBound instanceof Preceding
                || upperBound instanceof Following);
    if (!needsSingleOrdering) {
      return;
    }
    if (sorts.size() != 1) {
      throw new IllegalArgumentException(
          function
              + ": a RANGE bound with a Preceding or Following side requires exactly one ordering"
              + " expression, but found "
              + sorts.size());
    }
    if (sorts.get(0).direction() == Expression.SortDirection.CLUSTERED) {
      throw new IllegalArgumentException(
          function
              + ": a RANGE bound with a Preceding or Following side cannot use"
              + " SORT_DIRECTION_CLUSTERED for its ordering expression");
    }
  }

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

  /** A bound a fixed distance before the current row. */
  @Value.Immutable
  abstract class Preceding implements WindowBound {
    /**
     * Returns the expression evaluating to the distance preceding the current row.
     *
     * @return the offset expression
     */
    public abstract Expression offset();

    /**
     * Creates a {@link Preceding} bound from a literal {@code i64} row offset. Valid for ROWS, or
     * for RANGE over an {@code i64} ordering expression; use {@link #of(Expression)} otherwise.
     *
     * @param offset the row offset preceding the current row
     * @return the preceding bound
     */
    public static Preceding of(long offset) {
      return of(ExpressionCreator.i64(false, offset));
    }

    /**
     * Creates a {@link Preceding} bound with the given offset expression.
     *
     * @param offset the expression evaluating to the distance preceding the current row
     * @return the preceding bound
     */
    public static Preceding of(Expression offset) {
      return ImmutableWindowBound.Preceding.builder().offset(offset).build();
    }

    @Override
    public <R, E extends Throwable> R accept(WindowBoundVisitor<R, E> visitor) {
      return visitor.visit(this);
    }
  }

  /** A bound a fixed distance after the current row. */
  @Value.Immutable
  abstract class Following implements WindowBound {
    /**
     * Returns the expression evaluating to the distance following the current row.
     *
     * @return the offset expression
     */
    public abstract Expression offset();

    /**
     * Creates a {@link Following} bound from a literal {@code i64} row offset. Valid for ROWS, or
     * for RANGE over an {@code i64} ordering expression; use {@link #of(Expression)} otherwise.
     *
     * @param offset the row offset following the current row
     * @return the following bound
     */
    public static Following of(long offset) {
      return of(ExpressionCreator.i64(false, offset));
    }

    /**
     * Creates a {@link Following} bound with the given offset expression.
     *
     * @param offset the expression evaluating to the distance following the current row
     * @return the following bound
     */
    public static Following of(Expression offset) {
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
