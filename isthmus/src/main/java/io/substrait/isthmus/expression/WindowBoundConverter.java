package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.WindowBound;
import io.substrait.isthmus.TypeConverter;
import io.substrait.type.Type;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;

/**
 * Utility for converting Calcite {@link RexWindowBound} to Substrait {@link WindowBound}.
 *
 * <p>Supports {@code CURRENT ROW}, {@code UNBOUNDED}, and {@code PRECEDING}/{@code FOLLOWING}
 * bounds with an arbitrary offset expression.
 */
public class WindowBoundConverter {

  /**
   * Converts a Calcite {@link RexWindowBound} to a Substrait {@link WindowBound}.
   *
   * @param rexWindowBound the Calcite window bound to convert
   * @param isRows whether the enclosing frame is {@code BOUNDS_TYPE_ROWS} (as opposed to RANGE)
   * @param orderingType the type of the frame's single ordering expression; only consulted for a
   *     RANGE bound with an integral literal offset
   * @param rexExpressionConverter the converter used to convert a PRECEDING/FOLLOWING offset
   * @return the corresponding Substrait {@link WindowBound}
   * @throws IllegalStateException if the bound is not one of CURRENT ROW, UNBOUNDED, PRECEDING, or
   *     FOLLOWING
   */
  public static WindowBound toWindowBound(
      RexWindowBound rexWindowBound,
      boolean isRows,
      Optional<RelDataType> orderingType,
      RexExpressionConverter rexExpressionConverter) {
    if (rexWindowBound.isCurrentRow()) {
      return WindowBound.CURRENT_ROW;
    }
    if (rexWindowBound.isUnbounded()) {
      return WindowBound.UNBOUNDED;
    }

    RexNode node = rexWindowBound.getOffset();
    Expression offset =
        normalizeIntegralOffset(
            node.accept(rexExpressionConverter),
            isRows,
            orderingType,
            rexExpressionConverter.getTypeConverter());

    // Per the spec, zero is not a valid offset; it is equivalent to CurrentRow, and producers
    // should emit CurrentRow rather than a zero offset_expr.
    if (integralValue(offset).filter(value -> value == 0).isPresent()) {
      return WindowBound.CURRENT_ROW;
    }

    if (rexWindowBound.isPreceding()) {
      return WindowBound.Preceding.of(offset);
    }
    if (rexWindowBound.isFollowing()) {
      return WindowBound.Following.of(offset);
    }

    throw new IllegalStateException(
        "window bound was none of CURRENT ROW, UNBOUNDED, PRECEDING or FOLLOWING");
  }

  private static Expression normalizeIntegralOffset(
      Expression offset,
      boolean isRows,
      Optional<RelDataType> orderingType,
      TypeConverter typeConverter) {
    Optional<Long> value = integralValue(offset);
    if (value.isEmpty()) {
      return offset;
    }
    if (isRows) {
      // The spec requires a BOUNDS_TYPE_ROWS offset_expr to be int64.
      return ExpressionCreator.i64(false, value.get());
    }
    // BOUNDS_TYPE_RANGE: keep add(T, D) -> T defined for the ordering expression's type T.
    return orderingType
        .map(typeConverter::toSubstrait)
        .flatMap(type -> integralLiteralOfType(type, value.get()))
        .orElse(offset);
  }

  private static Optional<Long> integralValue(Expression expression) {
    if (expression instanceof Expression.I64Literal) {
      return Optional.of(((Expression.I64Literal) expression).value());
    } else if (expression instanceof Expression.I32Literal) {
      return Optional.of((long) ((Expression.I32Literal) expression).value());
    } else if (expression instanceof Expression.I16Literal) {
      return Optional.of((long) ((Expression.I16Literal) expression).value());
    } else if (expression instanceof Expression.I8Literal) {
      return Optional.of((long) ((Expression.I8Literal) expression).value());
    }
    return Optional.empty();
  }

  private static Optional<Expression> integralLiteralOfType(Type type, long value) {
    if (type instanceof Type.I64) {
      return Optional.of(ExpressionCreator.i64(false, value));
    } else if (type instanceof Type.I32) {
      return Optional.of(ExpressionCreator.i32(false, (int) value));
    } else if (type instanceof Type.I16) {
      return Optional.of(ExpressionCreator.i16(false, (int) value));
    } else if (type instanceof Type.I8) {
      return Optional.of(ExpressionCreator.i8(false, (int) value));
    }
    return Optional.empty();
  }
}
