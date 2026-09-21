package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.WindowBound;
import io.substrait.isthmus.TypeConverter;
import io.substrait.type.StringTypeVisitor;
import io.substrait.type.Type;
import io.substrait.util.DecimalUtil;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;

/**
 * Utility for converting Calcite {@link RexWindowBound} to Substrait {@link WindowBound}.
 *
 * <p>Supports {@code CURRENT ROW}, {@code UNBOUNDED}, and {@code PRECEDING}/{@code FOLLOWING}
 * bounds with an arbitrary offset expression. A RANGE bound's integral literal offset must match
 * the ordering expression's exact type. A negative integral offset is mirrored to the opposite
 * bound with its magnitude, and a zero offset becomes {@code CURRENT ROW}.
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
   * @throws UnsupportedOperationException if a RANGE offset's integral literal does not fit the
   *     ordering expression's exact type, or if a negative offset's magnitude has no positive
   *     representation
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
    Expression converted = node.accept(rexExpressionConverter);

    // Per the spec, zero is not a valid offset; it is equivalent to CurrentRow, and producers
    // should emit CurrentRow rather than a zero offset_expr. Checked before retyping: a zero
    // offset needs no representation in the ordering expression's type.
    if (integralValue(converted).filter(value -> value == 0).isPresent()) {
      return WindowBound.CURRENT_ROW;
    }

    // The spec carries a bound's direction in the Preceding/Following choice, not in the sign of
    // the offset: a negative offset is invalid, and the mirror bound with the magnitude is its
    // equivalent. Calcite only rejects a negative offset for ROWS, so RANGE reaches here.
    boolean preceding = rexWindowBound.isPreceding();
    Optional<Expression> negative = negateIfNegative(converted);
    if (negative.isPresent()) {
      preceding = !preceding;
      converted = negative.get();
    }

    Expression offset =
        normalizeIntegralOffset(
            converted, isRows, orderingType, rexExpressionConverter.getTypeConverter());

    if (preceding) {
      return WindowBound.Preceding.of(offset);
    }
    if (rexWindowBound.isFollowing() || negative.isPresent()) {
      return WindowBound.Following.of(offset);
    }

    throw new IllegalStateException(
        "window bound was none of CURRENT ROW, UNBOUNDED, PRECEDING or FOLLOWING");
  }

  /**
   * Returns the negation of {@code offset}, if it is a negative literal.
   *
   * @param offset the offset expression to check
   * @return the negated literal, or empty if {@code offset} is not a negative literal
   */
  private static Optional<Expression> negateIfNegative(Expression offset) {
    return decimalValue(offset)
        .filter(value -> value.signum() < 0)
        .map(value -> decimalLiteralOfType(offset.getType(), value.negate()))
        .or(
            () ->
                floatingValue(offset)
                    .filter(value -> value < 0)
                    .map(value -> floatingLiteralOfType(offset.getType(), -value)))
        .or(
            () ->
                integralValue(offset)
                    .filter(value -> value < 0)
                    .map(WindowBoundConverter::negateIntegral));
  }

  private static Optional<BigDecimal> decimalValue(Expression expression) {
    if (expression instanceof Expression.DecimalLiteral) {
      Expression.DecimalLiteral decimal = (Expression.DecimalLiteral) expression;
      return Optional.of(
          DecimalUtil.getBigDecimalFromBytes(decimal.value().toByteArray(), decimal.scale(), 16));
    }
    return Optional.empty();
  }

  private static Expression decimalLiteralOfType(Type type, BigDecimal value) {
    Type.Decimal decimal = (Type.Decimal) type;
    return ExpressionCreator.decimal(type.nullable(), value, decimal.precision(), decimal.scale());
  }

  private static Expression negateIntegral(long value) {
    long negated;
    try {
      negated = Math.negateExact(value);
    } catch (ArithmeticException e) {
      // Long.MIN_VALUE has no positive long representation.
      throw new UnsupportedOperationException("window offset " + value + " cannot be negated");
    }
    // Widen rather than negate in place: the magnitude need not fit the offset literal's own type,
    // only the type normalizeIntegralOffset then retypes it to.
    return ExpressionCreator.i64(false, negated);
  }

  private static Optional<Double> floatingValue(Expression expression) {
    if (expression instanceof Expression.FP64Literal) {
      return Optional.of(((Expression.FP64Literal) expression).value());
    } else if (expression instanceof Expression.FP32Literal) {
      return Optional.of((double) ((Expression.FP32Literal) expression).value());
    }
    return Optional.empty();
  }

  private static Expression floatingLiteralOfType(Type type, double value) {
    if (type instanceof Type.FP64) {
      return ExpressionCreator.fp64(type.nullable(), value);
    } else if (type instanceof Type.FP32) {
      return ExpressionCreator.fp32(type.nullable(), (float) value);
    }
    throw new IllegalStateException("expected a floating-point type, got " + type);
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
    // BOUNDS_TYPE_RANGE: an exact type match is isthmus's own policy, not a spec mandate.
    return orderingType
        .map(typeConverter::toSubstrait)
        .map(
            type ->
                integralLiteralOfType(type, value.get())
                    .orElseThrow(
                        () ->
                            new UnsupportedOperationException(
                                "RANGE window offset "
                                    + value.get()
                                    + " does not fit the ordering expression's type "
                                    + type.accept(new StringTypeVisitor()))))
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
      return fitsIn(value, Integer.MIN_VALUE, Integer.MAX_VALUE)
          ? Optional.of(ExpressionCreator.i32(false, (int) value))
          : Optional.empty();
    } else if (type instanceof Type.I16) {
      return fitsIn(value, Short.MIN_VALUE, Short.MAX_VALUE)
          ? Optional.of(ExpressionCreator.i16(false, (int) value))
          : Optional.empty();
    } else if (type instanceof Type.I8) {
      return fitsIn(value, Byte.MIN_VALUE, Byte.MAX_VALUE)
          ? Optional.of(ExpressionCreator.i8(false, (int) value))
          : Optional.empty();
    } else if (type instanceof Type.Decimal) {
      Type.Decimal decimal = (Type.Decimal) type;
      // encodeDecimalIntoBytes never checks the declared precision, only a fixed 16-byte cap.
      return digitCount(value) + decimal.scale() <= decimal.precision()
          ? Optional.of(
              ExpressionCreator.decimal(
                  false, BigDecimal.valueOf(value), decimal.precision(), decimal.scale()))
          : Optional.empty();
    } else if (type instanceof Type.FP64) {
      double asDouble = (double) value;
      return (long) asDouble == value
          ? Optional.of(ExpressionCreator.fp64(false, asDouble))
          : Optional.empty();
    } else if (type instanceof Type.FP32) {
      float asFloat = (float) value;
      return (long) asFloat == value
          ? Optional.of(ExpressionCreator.fp32(false, asFloat))
          : Optional.empty();
    }
    return Optional.empty();
  }

  private static boolean fitsIn(long value, long min, long max) {
    return value >= min && value <= max;
  }

  private static int digitCount(long value) {
    return BigInteger.valueOf(value).abs().toString().length();
  }
}
