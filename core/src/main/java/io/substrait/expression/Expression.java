package io.substrait.expression;

import com.google.protobuf.ByteString;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.AggregateFunction;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.util.VisitationContext;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.immutables.value.Value;

/**
 * Represents a Substrait expression that can be evaluated to produce a value. Expressions include
 * literals, field references, function invocations, and more complex constructs.
 */
@Value.Enclosing
public interface Expression extends FunctionArg {

  /**
   * Returns the type of this expression.
   *
   * @return a type
   */
  Type getType();

  @Override
  default <R, C extends VisitationContext, E extends Throwable> R accept(
      SimpleExtension.Function fnDef, int argIdx, FuncArgVisitor<R, C, E> fnArgVisitor, C context)
      throws E {
    return fnArgVisitor.visitExpr(fnDef, argIdx, this, context);
  }

  /** Marker interface for literal expressions that represent constant values. */
  interface Literal extends Expression {
    /**
     * Returns whether this literal can be null.
     *
     * @return true if this literal can be null, false otherwise
     */
    @Value.Default
    default boolean nullable() {
      return false;
    }

    /**
     * Returns a copy of this literal with the specified nullability.
     *
     * <p>This method is implemented by all concrete Literal classes via Immutables code generation.
     *
     * @param nullable whether the literal should be nullable
     * @return a copy of this literal with the specified nullability
     */
    Literal withNullable(boolean nullable);
  }

  /** Marker interface for nested expressions that contain other expressions. */
  interface Nested extends Expression {
    /**
     * Returns whether this nested expression can be null.
     *
     * @return true if this nested expression can be null, false otherwise
     */
    @Value.Default
    default boolean nullable() {
      return false;
    }
  }

  /**
   * Accepts a visitor to traverse this expression.
   *
   * @param <R> the return type of the visitor
   * @param <C> the context type
   * @param <E> the exception type that may be thrown
   * @param visitor the visitor to accept
   * @param context the visitation context
   * @return the result of the visitation
   * @throws E if an error occurs during visitation
   */
  <R, C extends VisitationContext, E extends Throwable> R accept(
      ExpressionVisitor<R, C, E> visitor, C context) throws E;

  /** Represents a null literal value with a specific type. */
  @Value.Immutable
  abstract class NullLiteral implements Literal {
    /**
     * Returns the type of this null literal.
     *
     * @return the type
     */
    public abstract Type type();

    /** A null literal is inherently nullable. You cannot have a null of a non nullable type. */
    @Override
    public boolean nullable() {
      return true;
    }

    @Override
    public NullLiteral withNullable(boolean nullable) {
      if (!nullable) {
        throw new IllegalArgumentException("NullLiteral cannot be made non-nullable");
      }
      return this;
    }

    /**
     * Validates that the type is nullable, as required for null literals.
     *
     * @throws IllegalArgumentException if the type is not nullable
     */
    @Value.Check
    protected void check() {
      if (!type().nullable()) {
        throw new IllegalArgumentException(
            "NullLiteral requires a nullable type, but got: " + type());
      }
    }

    @Override
    public Type getType() {
      return type();
    }

    /**
     * Creates a new builder for constructing a NullLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.NullLiteral.Builder builder() {
      return ImmutableExpression.NullLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a boolean literal value. */
  @Value.Immutable
  abstract class BoolLiteral implements Literal {
    /**
     * Returns the boolean value of this literal.
     *
     * @return the boolean value
     */
    public abstract Boolean value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).BOOLEAN;
    }

    /**
     * Creates a new builder for constructing a BoolLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.BoolLiteral.Builder builder() {
      return ImmutableExpression.BoolLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents an 8-bit signed integer literal. */
  @Value.Immutable
  abstract class I8Literal implements Literal {
    /**
     * Returns the 8-bit integer value of this literal.
     *
     * @return the integer value
     */
    public abstract int value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).I8;
    }

    /**
     * Creates a new builder for constructing an I8Literal.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.I8Literal.Builder builder() {
      return ImmutableExpression.I8Literal.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a 16-bit signed integer literal. */
  @Value.Immutable
  abstract class I16Literal implements Literal {
    /**
     * Returns the 16-bit integer value of this literal.
     *
     * @return the integer value
     */
    public abstract int value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).I16;
    }

    /**
     * Creates a new builder for constructing an I16Literal.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.I16Literal.Builder builder() {
      return ImmutableExpression.I16Literal.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a 32-bit signed integer literal. */
  @Value.Immutable
  abstract class I32Literal implements Literal {
    /**
     * Returns the 32-bit integer value of this literal.
     *
     * @return the integer value
     */
    public abstract int value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).I32;
    }

    /**
     * Creates a new builder for constructing an I32Literal.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.I32Literal.Builder builder() {
      return ImmutableExpression.I32Literal.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a 64-bit signed integer literal. */
  @Value.Immutable
  abstract class I64Literal implements Literal {
    /**
     * Returns the 64-bit integer value of this literal.
     *
     * @return the long value
     */
    public abstract long value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).I64;
    }

    /**
     * Creates a new builder for constructing an I64Literal.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.I64Literal.Builder builder() {
      return ImmutableExpression.I64Literal.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a 32-bit floating point literal. */
  @Value.Immutable
  abstract class FP32Literal implements Literal {
    /**
     * Returns the 32-bit floating point value of this literal.
     *
     * @return the float value
     */
    public abstract float value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).FP32;
    }

    /**
     * Creates a new builder for constructing an FP32Literal.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.FP32Literal.Builder builder() {
      return ImmutableExpression.FP32Literal.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a 64-bit floating point literal. */
  @Value.Immutable
  abstract class FP64Literal implements Literal {
    /**
     * Returns the 64-bit floating point value of this literal.
     *
     * @return the double value
     */
    public abstract double value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).FP64;
    }

    /**
     * Creates a new builder for constructing an FP64Literal.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.FP64Literal.Builder builder() {
      return ImmutableExpression.FP64Literal.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a string literal value. */
  @Value.Immutable
  abstract class StrLiteral implements Literal {
    /**
     * Returns the string value of this literal.
     *
     * @return the string value
     */
    public abstract String value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).STRING;
    }

    /**
     * Creates a new builder for constructing a StrLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.StrLiteral.Builder builder() {
      return ImmutableExpression.StrLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a binary (byte array) literal value. */
  @Value.Immutable
  abstract class BinaryLiteral implements Literal {
    /**
     * Returns the binary value of this literal.
     *
     * @return the binary value as a ByteString
     */
    public abstract ByteString value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).BINARY;
    }

    /**
     * Creates a new builder for constructing a BinaryLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.BinaryLiteral.Builder builder() {
      return ImmutableExpression.BinaryLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /**
   * @deprecated {@link TimestampLiteral} is deprecated in favor of {@link
   *     PrecisionTimestampLiteral}
   */
  @Value.Immutable
  @Deprecated
  abstract class TimestampLiteral implements Literal {
    /**
     * Returns the timestamp value in microseconds since epoch.
     *
     * @return the timestamp value
     */
    public abstract long value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).TIMESTAMP;
    }

    /**
     * Creates a new builder for constructing a TimestampLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.TimestampLiteral.Builder builder() {
      return ImmutableExpression.TimestampLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /**
   * @deprecated {@link TimeLiteral} is deprecated in favor of {@link PrecisionTimeLiteral}
   */
  @Value.Immutable
  @Deprecated
  abstract class TimeLiteral implements Literal {
    /**
     * Returns the time value in microseconds since midnight.
     *
     * @return the time value
     */
    public abstract long value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).TIME;
    }

    /**
     * Creates a new builder for constructing a TimeLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.TimeLiteral.Builder builder() {
      return ImmutableExpression.TimeLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a time literal with configurable precision. */
  @Value.Immutable
  abstract class PrecisionTimeLiteral implements Literal {
    /**
     * Returns the time value since midnight.
     *
     * @return the time value
     */
    public abstract long value();

    /**
     * Returns the precision of the time literal.
     *
     * @return the precision
     */
    public abstract int precision();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).precisionTimestamp(precision());
    }

    /**
     * Creates a new builder for constructing a PrecisionTimeLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.PrecisionTimeLiteral.Builder builder() {
      return ImmutableExpression.PrecisionTimeLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a date literal value. */
  @Value.Immutable
  abstract class DateLiteral implements Literal {
    /**
     * Returns the date value as days since epoch.
     *
     * @return the date value
     */
    public abstract int value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).DATE;
    }

    /**
     * Creates a new builder for constructing a DateLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.DateLiteral.Builder builder() {
      return ImmutableExpression.DateLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /**
   * @deprecated {@link TimestampTZLiteral} is deprecated in favor of {@link
   *     PrecisionTimestampTZLiteral}
   */
  @Value.Immutable
  @Deprecated
  abstract class TimestampTZLiteral implements Literal {
    /**
     * Returns the timestamp with timezone value in microseconds since epoch.
     *
     * @return the timestamp value
     */
    public abstract long value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).TIMESTAMP_TZ;
    }

    /**
     * Creates a new builder for constructing a TimestampTZLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.TimestampTZLiteral.Builder builder() {
      return ImmutableExpression.TimestampTZLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a timestamp literal with configurable precision. */
  @Value.Immutable
  abstract class PrecisionTimestampLiteral implements Literal {
    /**
     * Returns the timestamp value since epoch.
     *
     * @return the timestamp value
     */
    public abstract long value();

    /**
     * Returns the precision of the timestamp literal.
     *
     * @return the precision
     */
    public abstract int precision();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).precisionTimestamp(precision());
    }

    /**
     * Creates a new builder for constructing a PrecisionTimestampLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.PrecisionTimestampLiteral.Builder builder() {
      return ImmutableExpression.PrecisionTimestampLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a timestamp with timezone literal with configurable precision. */
  @Value.Immutable
  abstract class PrecisionTimestampTZLiteral implements Literal {
    /**
     * Returns the timestamp with timezone value since epoch.
     *
     * @return the timestamp value
     */
    public abstract long value();

    /**
     * Returns the precision of the timestamp with timezone literal.
     *
     * @return the precision
     */
    public abstract int precision();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).precisionTimestampTZ(precision());
    }

    /**
     * Creates a new builder for constructing a PrecisionTimestampTZLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.PrecisionTimestampTZLiteral.Builder builder() {
      return ImmutableExpression.PrecisionTimestampTZLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents an interval literal measured in years and months. */
  @Value.Immutable
  abstract class IntervalYearLiteral implements Literal {
    /**
     * Returns the years component of this interval.
     *
     * @return the years value
     */
    public abstract int years();

    /**
     * Returns the months component of this interval.
     *
     * @return the months value
     */
    public abstract int months();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).INTERVAL_YEAR;
    }

    /**
     * Creates a new builder for constructing an IntervalYearLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.IntervalYearLiteral.Builder builder() {
      return ImmutableExpression.IntervalYearLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /**
   * Represents an interval literal measured in days, seconds, and subseconds with configurable
   * precision.
   */
  @Value.Immutable
  abstract class IntervalDayLiteral implements Literal {
    /**
     * Returns the days component of this interval.
     *
     * @return the days value
     */
    public abstract int days();

    /**
     * Returns the seconds component of this interval.
     *
     * @return the seconds value
     */
    public abstract int seconds();

    /**
     * Returns the subseconds component of this interval.
     *
     * @return the subseconds value
     */
    public abstract long subseconds();

    /**
     * Returns the precision of the subseconds component.
     *
     * @return the precision value
     */
    public abstract int precision();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).intervalDay(precision());
    }

    /**
     * Creates a new builder for constructing an IntervalDayLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.IntervalDayLiteral.Builder builder() {
      return ImmutableExpression.IntervalDayLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a compound interval literal combining year-month and day-time components. */
  @Value.Immutable
  abstract class IntervalCompoundLiteral implements Literal {
    // Flattened IntervalYearLiteral
    /**
     * Returns the years component of this interval.
     *
     * @return the years value
     */
    public abstract int years();

    /**
     * Returns the months component of this interval.
     *
     * @return the months value
     */
    public abstract int months();

    // Flattened IntervalDayLiteral
    /**
     * Returns the days component of this interval.
     *
     * @return the days value
     */
    public abstract int days();

    /**
     * Returns the seconds component of this interval.
     *
     * @return the seconds value
     */
    public abstract int seconds();

    /**
     * Returns the subseconds component of this interval.
     *
     * @return the subseconds value
     */
    public abstract long subseconds();

    /**
     * Returns the precision of the subseconds component.
     *
     * @return the precision value
     */
    public abstract int precision();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).intervalCompound(precision());
    }

    /**
     * Creates a new builder for constructing an IntervalCompoundLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.IntervalCompoundLiteral.Builder builder() {
      return ImmutableExpression.IntervalCompoundLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a UUID (Universally Unique Identifier) literal value. */
  @Value.Immutable
  abstract class UUIDLiteral implements Literal {
    /**
     * Returns the UUID value.
     *
     * @return the UUID value
     */
    public abstract UUID value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).UUID;
    }

    /**
     * Creates a new builder for constructing a UUIDLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.UUIDLiteral.Builder builder() {
      return ImmutableExpression.UUIDLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }

    /**
     * Converts the UUID to a ByteString representation.
     *
     * @return the UUID as a ByteString
     */
    public ByteString toBytes() {
      ByteBuffer bb = ByteBuffer.allocate(16);
      bb.putLong(value().getMostSignificantBits());
      bb.putLong(value().getLeastSignificantBits());
      bb.flip();
      return ByteString.copyFrom(bb);
    }
  }

  /** Represents a fixed-length character string literal. */
  @Value.Immutable
  abstract class FixedCharLiteral implements Literal {
    /**
     * Returns the fixed-length character string value.
     *
     * @return the string value
     */
    public abstract String value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).fixedChar(value().length());
    }

    /**
     * Creates a new builder for constructing a FixedCharLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.FixedCharLiteral.Builder builder() {
      return ImmutableExpression.FixedCharLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a variable-length character string literal with a maximum length. */
  @Value.Immutable
  abstract class VarCharLiteral implements Literal {
    /**
     * Returns the variable-length character string value.
     *
     * @return the string value
     */
    public abstract String value();

    /**
     * Returns the maximum length of the string.
     *
     * @return the maximum length
     */
    public abstract int length();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).varChar(length());
    }

    /**
     * Creates a new builder for constructing a VarCharLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.VarCharLiteral.Builder builder() {
      return ImmutableExpression.VarCharLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a fixed-length binary literal. */
  @Value.Immutable
  abstract class FixedBinaryLiteral implements Literal {
    /**
     * Returns the fixed-length binary value.
     *
     * @return the binary value as a ByteString
     */
    public abstract ByteString value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).fixedBinary(value().size());
    }

    /**
     * Creates a new builder for constructing a FixedBinaryLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.FixedBinaryLiteral.Builder builder() {
      return ImmutableExpression.FixedBinaryLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a decimal literal with configurable precision and scale. */
  @Value.Immutable
  abstract class DecimalLiteral implements Literal {
    /**
     * Returns the decimal value as a ByteString.
     *
     * @return the decimal value
     */
    public abstract ByteString value();

    /**
     * Returns the precision of this decimal.
     *
     * @return the precision value
     */
    public abstract int precision();

    /**
     * Returns the scale of this decimal.
     *
     * @return the scale value
     */
    public abstract int scale();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).decimal(precision(), scale());
    }

    /**
     * Creates a new builder for constructing a DecimalLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.DecimalLiteral.Builder builder() {
      return ImmutableExpression.DecimalLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a map literal with key-value pairs. */
  @Value.Immutable
  abstract class MapLiteral implements Literal {
    /**
     * Returns the map of key-value pairs.
     *
     * @return the map values
     */
    public abstract Map<Literal, Literal> values();

    @Override
    public Type getType() {
      return Type.withNullability(nullable())
          .map(
              values().keySet().iterator().next().getType(),
              values().values().iterator().next().getType());
    }

    /**
     * Creates a new builder for constructing a MapLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.MapLiteral.Builder builder() {
      return ImmutableExpression.MapLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents an empty map literal with specified key and value types. */
  @Value.Immutable
  abstract class EmptyMapLiteral implements Literal {
    /**
     * Returns the type of keys in this empty map.
     *
     * @return the key type
     */
    public abstract Type keyType();

    /**
     * Returns the type of values in this empty map.
     *
     * @return the value type
     */
    public abstract Type valueType();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).map(keyType(), valueType());
    }

    /**
     * Creates a new builder for constructing an EmptyMapLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.EmptyMapLiteral.Builder builder() {
      return ImmutableExpression.EmptyMapLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a list literal containing multiple values. */
  @Value.Immutable
  abstract class ListLiteral implements Literal {
    /**
     * Returns the list of literal values.
     *
     * @return the list values
     */
    public abstract List<Literal> values();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).list(values().get(0).getType());
    }

    /**
     * Creates a new builder for constructing a ListLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.ListLiteral.Builder builder() {
      return ImmutableExpression.ListLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents an empty list literal with a specified element type. */
  @Value.Immutable
  abstract class EmptyListLiteral implements Literal {
    /**
     * Returns the type of elements in this empty list.
     *
     * @return the element type
     */
    public abstract Type elementType();

    @Override
    public Type.ListType getType() {
      return Type.withNullability(nullable()).list(elementType());
    }

    /**
     * Creates a new builder for constructing an EmptyListLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.EmptyListLiteral.Builder builder() {
      return ImmutableExpression.EmptyListLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a struct literal with multiple field values. */
  @Value.Immutable
  abstract class StructLiteral implements Literal {
    /**
     * Returns the list of field literals in this struct.
     *
     * @return the field literals
     */
    public abstract List<Literal> fields();

    @Override
    public Type getType() {
      return Type.withNullability(nullable())
          .struct(
              fields().stream()
                  .map(Literal::getType)
                  .collect(java.util.stream.Collectors.toList()));
    }

    /**
     * Creates a new builder for constructing a StructLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.StructLiteral.Builder builder() {
      return ImmutableExpression.StructLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a nested struct expression with multiple field expressions. */
  @Value.Immutable
  abstract class NestedStruct implements Nested {
    /**
     * Returns the list of field expressions in this nested struct.
     *
     * @return the field expressions
     */
    public abstract List<Expression> fields();

    @Override
    public Type getType() {
      return Type.withNullability(nullable())
          .struct(
              fields().stream()
                  .map(Expression::getType)
                  .collect(java.util.stream.Collectors.toList()));
    }

    /**
     * Creates a new builder for constructing a NestedStruct.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.NestedStruct.Builder builder() {
      return ImmutableExpression.NestedStruct.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a lambda expression with parameters and a body. */
  @Value.Immutable
  abstract class Lambda implements Expression {
    /**
     * Returns the parameters of this lambda expression.
     *
     * @return the parameter types as a struct
     */
    public abstract Type.Struct parameters();

    /**
     * Returns the body expression of this lambda.
     *
     * @return the body expression
     */
    public abstract Expression body();

    @Override
    public Type getType() {
      List<Type> paramTypes = parameters().fields();
      Type returnType = body().getType();

      // TODO: Type.Func nullability is hardcoded to false here because the spec does not allow for
      //   declaring otherwise.
      // See: https://github.com/substrait-io/substrait/issues/976
      return Type.withNullability(false).func(paramTypes, returnType);
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /**
   * Base interface for user-defined literals.
   *
   * <p>User-defined literals can be encoded in one of two ways as per the Substrait spec:
   *
   * <ul>
   *   <li>As {@code google.protobuf.Any} - see {@link UserDefinedAnyLiteral}
   *   <li>As {@code Literal.Struct} - see {@link UserDefinedStructLiteral}
   * </ul>
   */
  interface UserDefinedLiteral extends Literal {
    /**
     * Returns the URN (Uniform Resource Name) identifying the user-defined type.
     *
     * @return the URN
     */
    String urn();

    /**
     * Returns the name of the user-defined type.
     *
     * @return the type name
     */
    String name();

    /**
     * Returns the list of type parameters for this user-defined type.
     *
     * @return the type parameters
     */
    List<io.substrait.type.Type.Parameter> typeParameters();
  }

  /**
   * User-defined literal with value encoded as {@link com.google.protobuf.Any}.
   *
   * <p>This encoding allows for arbitrary binary data to be stored in the literal value.
   */
  @Value.Immutable
  abstract class UserDefinedAnyLiteral implements UserDefinedLiteral {
    @Override
    public abstract String urn();

    @Override
    public abstract String name();

    @Override
    public abstract List<io.substrait.type.Type.Parameter> typeParameters();

    /**
     * Returns the value encoded as a protobuf Any.
     *
     * @return the encoded value
     */
    public abstract com.google.protobuf.Any value();

    @Override
    public Type.UserDefined getType() {
      return Type.UserDefined.builder()
          .nullable(nullable())
          .urn(urn())
          .name(name())
          .typeParameters(typeParameters())
          .build();
    }

    /**
     * Creates a new builder for constructing a UserDefinedAnyLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.UserDefinedAnyLiteral.Builder builder() {
      return ImmutableExpression.UserDefinedAnyLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /**
   * User-defined literal with value encoded as {@link
   * io.substrait.proto.Expression.Literal.Struct}.
   *
   * <p>This encoding uses a structured list of fields to represent the literal value.
   */
  @Value.Immutable
  abstract class UserDefinedStructLiteral implements UserDefinedLiteral {
    @Override
    public abstract String urn();

    @Override
    public abstract String name();

    @Override
    public abstract List<io.substrait.type.Type.Parameter> typeParameters();

    /**
     * Returns the list of field literals in this user-defined struct.
     *
     * @return the field literals
     */
    public abstract List<Literal> fields();

    @Override
    public Type.UserDefined getType() {
      return Type.UserDefined.builder()
          .nullable(nullable())
          .urn(urn())
          .name(name())
          .typeParameters(typeParameters())
          .build();
    }

    /**
     * Creates a new builder for constructing a UserDefinedStructLiteral.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.UserDefinedStructLiteral.Builder builder() {
      return ImmutableExpression.UserDefinedStructLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a switch expression that evaluates different cases based on a match value. */
  @Value.Immutable
  abstract class Switch implements Expression {
    /**
     * Returns the expression to match against.
     *
     * @return the match expression
     */
    public abstract Expression match();

    /**
     * Returns the list of switch case clauses.
     *
     * @return the switch clauses
     */
    public abstract List<SwitchClause> switchClauses();

    /**
     * Returns the default expression when no switch case clauses match.
     *
     * @return the default clause expression
     */
    public abstract Expression defaultClause();

    @Override
    public Type getType() {
      return defaultClause().getType();
    }

    /**
     * Creates a new builder for constructing a Switch.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.Switch.Builder builder() {
      return ImmutableExpression.Switch.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a single case clause in a switch expression. */
  @Value.Immutable
  abstract class SwitchClause {
    /**
     * Returns the condition literal to match.
     *
     * @return the condition literal
     */
    public abstract Literal condition();

    /**
     * Returns the expression to evaluate when the condition matches.
     *
     * @return the then expression
     */
    public abstract Expression then();

    /**
     * Creates a new builder for constructing a SwitchClause.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.SwitchClause.Builder builder() {
      return ImmutableExpression.SwitchClause.builder();
    }
  }

  /** Represents an if-then-else conditional expression with multiple if clauses. */
  @Value.Immutable
  abstract class IfThen implements Expression {
    /**
     * Returns the list of if-then clauses.
     *
     * @return the list of if clauses
     */
    public abstract List<IfClause> ifClauses();

    /**
     * Returns the else clause expression.
     *
     * @return the else clause expression
     */
    public abstract Expression elseClause();

    @Override
    public Type getType() {
      Type elseType = elseClause().getType();

      // If any of the clauses are nullable, the whole expression is also nullable.
      if (ifClauses().stream().anyMatch(clause -> clause.then().getType().nullable())) {
        return TypeCreator.asNullable(elseType);
      }
      return elseType;
    }

    /**
     * Creates a new builder for constructing an IfThen expression.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.IfThen.Builder builder() {
      return ImmutableExpression.IfThen.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a single if-then clause in a conditional expression. */
  @Value.Immutable
  abstract class IfClause {
    /**
     * Returns the condition expression for this if clause.
     *
     * @return the condition expression
     */
    public abstract Expression condition();

    /**
     * Returns the then expression for this if clause.
     *
     * @return the then expression
     */
    public abstract Expression then();

    /**
     * Creates a new builder for constructing an IfClause.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.IfClause.Builder builder() {
      return ImmutableExpression.IfClause.builder();
    }
  }

  /** Represents a type cast expression that converts a value from one type to another. */
  @Value.Immutable
  abstract class Cast implements Expression {
    /**
     * Returns the target type for this cast.
     *
     * @return the target type
     */
    public abstract Type type();

    /**
     * Returns the input expression to be cast.
     *
     * @return the input expression
     */
    public abstract Expression input();

    /**
     * Returns the failure behavior for this cast.
     *
     * @return the failure behavior
     */
    public abstract FailureBehavior failureBehavior();

    @Override
    public Type getType() {
      return type();
    }

    /**
     * Creates a new builder for constructing a Cast expression.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.Cast.Builder builder() {
      return ImmutableExpression.Cast.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents an invocation of a scalar function that returns a single value. */
  @Value.Immutable
  abstract class ScalarFunctionInvocation implements Expression {
    /**
     * Returns the function declaration for this scalar function invocation.
     *
     * @return the function declaration
     */
    public abstract SimpleExtension.ScalarFunctionVariant declaration();

    /**
     * Returns the list of arguments passed to this function.
     *
     * @return the function arguments
     */
    public abstract List<FunctionArg> arguments();

    /**
     * Returns the list of options for this function invocation.
     *
     * @return the function options
     */
    public abstract List<FunctionOption> options();

    /**
     * Returns the output type of this function invocation.
     *
     * @return the output type
     */
    public abstract Type outputType();

    @Override
    public Type getType() {
      return outputType();
    }

    /**
     * Validates that variadic arguments satisfy the parameter consistency requirement. When
     * CONSISTENT, all variadic arguments must have the same type (ignoring nullability). When
     * INCONSISTENT, arguments can have different types.
     */
    @Value.Check
    protected void check() {
      VariadicParameterConsistencyValidator.validate(declaration(), arguments());
    }

    /**
     * Creates a new builder for constructing a ScalarFunctionInvocation.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.ScalarFunctionInvocation.Builder builder() {
      return ImmutableExpression.ScalarFunctionInvocation.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /**
   * Represents an invocation of a window function with partitioning, ordering, and frame bounds.
   */
  @Value.Immutable
  abstract class WindowFunctionInvocation implements Expression {

    /**
     * Returns the window function variant declaration.
     *
     * @return the window function variant
     */
    public abstract SimpleExtension.WindowFunctionVariant declaration();

    /**
     * Returns the list of arguments passed to this window function.
     *
     * @return the function arguments
     */
    public abstract List<FunctionArg> arguments();

    /**
     * Returns the list of options for this window function invocation.
     *
     * @return the function options
     */
    public abstract List<FunctionOption> options();

    /**
     * Returns the aggregation phase for this window function.
     *
     * @return the aggregation phase
     */
    public abstract AggregationPhase aggregationPhase();

    /**
     * Returns the list of expressions to partition by.
     *
     * @return the partition by expressions
     */
    public abstract List<Expression> partitionBy();

    /**
     * Returns the list of sort fields for ordering.
     *
     * @return the sort fields
     */
    public abstract List<SortField> sort();

    /**
     * Returns the window bounds type (rows or range).
     *
     * @return the bounds type
     */
    public abstract WindowBoundsType boundsType();

    /**
     * Returns the lower bound of the window frame.
     *
     * @return the lower bound
     */
    public abstract WindowBound lowerBound();

    /**
     * Returns the upper bound of the window frame.
     *
     * @return the upper bound
     */
    public abstract WindowBound upperBound();

    /**
     * Returns the output type of this window function.
     *
     * @return the output type
     */
    public abstract Type outputType();

    @Override
    public Type getType() {
      return outputType();
    }

    /**
     * Returns the aggregation invocation mode.
     *
     * @return the aggregation invocation
     */
    public abstract AggregationInvocation invocation();

    /**
     * Validates that variadic arguments satisfy the parameter consistency requirement. When
     * CONSISTENT, all variadic arguments must have the same type (ignoring nullability). When
     * INCONSISTENT, arguments can have different types.
     */
    @Value.Check
    protected void check() {
      VariadicParameterConsistencyValidator.validate(declaration(), arguments());
    }

    /**
     * Creates a new builder for constructing a WindowFunctionInvocation.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.WindowFunctionInvocation.Builder builder() {
      return ImmutableExpression.WindowFunctionInvocation.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Defines the type of window bounds (rows or range) for window functions. */
  enum WindowBoundsType {
    /** Unspecified window bounds type. */
    UNSPECIFIED(io.substrait.proto.Expression.WindowFunction.BoundsType.BOUNDS_TYPE_UNSPECIFIED),
    /** Window bounds based on row count. */
    ROWS(io.substrait.proto.Expression.WindowFunction.BoundsType.BOUNDS_TYPE_ROWS),
    /** Window bounds based on value range. */
    RANGE(io.substrait.proto.Expression.WindowFunction.BoundsType.BOUNDS_TYPE_RANGE);

    private final io.substrait.proto.Expression.WindowFunction.BoundsType proto;

    WindowBoundsType(io.substrait.proto.Expression.WindowFunction.BoundsType proto) {
      this.proto = proto;
    }

    /**
     * Converts this window bounds type to its protobuf representation.
     *
     * @return the protobuf representation
     */
    public io.substrait.proto.Expression.WindowFunction.BoundsType toProto() {
      return proto;
    }

    /**
     * Converts a protobuf window bounds type to this enum.
     *
     * @param proto the protobuf representation
     * @return the corresponding enum value
     */
    public static WindowBoundsType fromProto(
        io.substrait.proto.Expression.WindowFunction.BoundsType proto) {
      for (WindowBoundsType v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }

      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  /**
   * Represents a single-or-list expression that checks if a condition matches any option in a list.
   */
  @Value.Immutable
  abstract class SingleOrList implements Expression {
    /**
     * Returns the condition expression to match against.
     *
     * @return the condition expression
     */
    public abstract Expression condition();

    /**
     * Returns the list of option expressions to check.
     *
     * @return the option expressions
     */
    public abstract List<Expression> options();

    @Override
    public Type getType() {
      return TypeCreator.NULLABLE.BOOLEAN;
    }

    /**
     * Creates a new builder for constructing a SingleOrList.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.SingleOrList.Builder builder() {
      return ImmutableExpression.SingleOrList.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /**
   * Represents a multi-or-list expression that checks multiple conditions against combinations of
   * options.
   */
  @Value.Immutable
  abstract class MultiOrList implements Expression {
    /**
     * Returns the list of condition expressions.
     *
     * @return the list of conditions
     */
    public abstract List<Expression> conditions();

    /**
     * Returns the list of option combinations.
     *
     * @return the list of option combinations
     */
    public abstract List<MultiOrListRecord> optionCombinations();

    @Override
    public Type getType() {
      return TypeCreator.NULLABLE.BOOLEAN;
    }

    /**
     * Creates a new builder for constructing a MultiOrList expression.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.MultiOrList.Builder builder() {
      return ImmutableExpression.MultiOrList.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /**
   * A nested list expression with one or more elements.
   *
   * <p>Note: This class cannot be used to construct an empty list. To create an empty list, use
   * {@link ExpressionCreator#emptyList(boolean, Type)} which returns an {@link EmptyListLiteral}.
   */
  @Value.Immutable
  abstract class NestedList implements Nested {
    /**
     * Returns the list of expression values in this nested list.
     *
     * @return the list of values
     */
    public abstract List<Expression> values();

    /** Validates that the nested list is not empty and all values have the same type. */
    @Value.Check
    protected void check() {
      assert !values().isEmpty() : "To specify an empty list, use ExpressionCreator.emptyList()";

      assert values().stream().map(Expression::getType).distinct().count() <= 1
          : "All values in NestedList must have the same type";
    }

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).list(values().get(0).getType());
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }

    /**
     * Creates a new builder for constructing a NestedList.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.NestedList.Builder builder() {
      return ImmutableExpression.NestedList.builder();
    }
  }

  /** Represents a single record (combination of values) in a multi-or-list expression. */
  @Value.Immutable
  abstract class MultiOrListRecord {
    /**
     * Returns the list of expression values.
     *
     * @return the list of values
     */
    public abstract List<Expression> values();

    /**
     * Creates a new builder for constructing a MultiOrListRecord.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.MultiOrListRecord.Builder builder() {
      return ImmutableExpression.MultiOrListRecord.builder();
    }
  }

  /** Represents a sort field with an expression and sort direction. */
  @Value.Immutable
  abstract class SortField {
    /**
     * Returns the expression to sort by.
     *
     * @return the sort expression
     */
    public abstract Expression expr();

    /**
     * Returns the sort direction.
     *
     * @return the sort direction
     */
    public abstract SortDirection direction();

    /**
     * Creates a new builder for constructing a SortField.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.SortField.Builder builder() {
      return ImmutableExpression.SortField.builder();
    }
  }

  /** Base interface for subquery expressions. */
  interface Subquery extends Expression {}

  /** Represents a set predicate subquery. */
  @Value.Immutable
  abstract class SetPredicate implements Subquery {
    /**
     * Returns the predicate operation type.
     *
     * @return the predicate operation
     */
    public abstract PredicateOp predicateOp();

    /**
     * Returns the relation containing the tuples to check.
     *
     * @return the tuples relation
     */
    public abstract Rel tuples();

    @Override
    public Type getType() {
      return TypeCreator.REQUIRED.BOOLEAN;
    }

    /**
     * Creates a new builder for constructing a SetPredicate.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.SetPredicate.Builder builder() {
      return ImmutableExpression.SetPredicate.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents a scalar subquery that returns a single value. */
  @Value.Immutable
  abstract class ScalarSubquery implements Subquery {
    /**
     * Returns the input relation for this scalar subquery.
     *
     * @return the input relation
     */
    public abstract Rel input();

    /**
     * Creates a new builder for constructing a ScalarSubquery.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.ScalarSubquery.Builder builder() {
      return ImmutableExpression.ScalarSubquery.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Represents an IN predicate that checks if values exist in a subquery result. */
  @Value.Immutable
  abstract class InPredicate implements Subquery {
    /**
     * Returns the relation to search in (the haystack).
     *
     * @return the haystack relation
     */
    public abstract Rel haystack();

    /**
     * Returns the list of expressions to search for (the needles).
     *
     * @return the list of needle expressions
     */
    public abstract List<Expression> needles();

    @Override
    public Type getType() {
      return TypeCreator.REQUIRED.BOOLEAN;
    }

    /**
     * Creates a new builder for constructing an InPredicate expression.
     *
     * @return a new builder instance
     */
    public static ImmutableExpression.InPredicate.Builder builder() {
      return ImmutableExpression.InPredicate.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  /** Defines the operation type for set predicates (EXISTS, UNIQUE). */
  enum PredicateOp {
    /** Unspecified predicate operation. */
    PREDICATE_OP_UNSPECIFIED(
        io.substrait.proto.Expression.Subquery.SetPredicate.PredicateOp.PREDICATE_OP_UNSPECIFIED),
    /** Checks if the subquery returns any rows (EXISTS). */
    PREDICATE_OP_EXISTS(
        io.substrait.proto.Expression.Subquery.SetPredicate.PredicateOp.PREDICATE_OP_EXISTS),
    /** Checks if all rows in the subquery are unique. */
    PREDICATE_OP_UNIQUE(
        io.substrait.proto.Expression.Subquery.SetPredicate.PredicateOp.PREDICATE_OP_UNIQUE);

    private final io.substrait.proto.Expression.Subquery.SetPredicate.PredicateOp proto;

    PredicateOp(io.substrait.proto.Expression.Subquery.SetPredicate.PredicateOp proto) {
      this.proto = proto;
    }

    /**
     * Converts this predicate operation to its protobuf representation.
     *
     * @return the protobuf representation
     */
    public io.substrait.proto.Expression.Subquery.SetPredicate.PredicateOp toProto() {
      return proto;
    }

    /**
     * Converts a protobuf predicate operation to this enum.
     *
     * @param proto the protobuf representation
     * @return the corresponding enum value
     */
    public static PredicateOp fromProto(
        io.substrait.proto.Expression.Subquery.SetPredicate.PredicateOp proto) {
      for (PredicateOp v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }

      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  /** Defines how aggregation functions are invoked (ALL, DISTINCT). */
  enum AggregationInvocation {
    /** Unspecified aggregation invocation. */
    UNSPECIFIED(AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_UNSPECIFIED),
    /** Aggregate over all values. */
    ALL(AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_ALL),
    /** Aggregate over distinct values only. */
    DISTINCT(AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_DISTINCT);

    private final io.substrait.proto.AggregateFunction.AggregationInvocation proto;

    AggregationInvocation(io.substrait.proto.AggregateFunction.AggregationInvocation proto) {
      this.proto = proto;
    }

    /**
     * Converts this aggregation invocation to its protobuf representation.
     *
     * @return the protobuf representation
     */
    public io.substrait.proto.AggregateFunction.AggregationInvocation toProto() {
      return proto;
    }

    /**
     * Converts a protobuf aggregation invocation to this enum.
     *
     * @param proto the protobuf representation
     * @return the corresponding enum value
     */
    public static AggregationInvocation fromProto(AggregateFunction.AggregationInvocation proto) {
      for (AggregationInvocation v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }

      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  /** Defines the phase of aggregation execution (initial, intermediate, or result). */
  enum AggregationPhase {
    /** Unspecified aggregation phase. */
    UNSPECIFIED(io.substrait.proto.AggregationPhase.AGGREGATION_PHASE_UNSPECIFIED),
    /** Aggregation from initial input to intermediate state. */
    INITIAL_TO_INTERMEDIATE(
        io.substrait.proto.AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE),
    /** Aggregation from intermediate state to intermediate state. */
    INTERMEDIATE_TO_INTERMEDIATE(
        io.substrait.proto.AggregationPhase.AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE),
    /** Aggregation from initial input directly to final result. */
    INITIAL_TO_RESULT(io.substrait.proto.AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_RESULT),
    /** Aggregation from intermediate state to final result. */
    INTERMEDIATE_TO_RESULT(
        io.substrait.proto.AggregationPhase.AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT);

    private final io.substrait.proto.AggregationPhase proto;

    AggregationPhase(io.substrait.proto.AggregationPhase proto) {
      this.proto = proto;
    }

    /**
     * Converts this aggregation phase to its protobuf representation.
     *
     * @return the protobuf representation
     */
    public io.substrait.proto.AggregationPhase toProto() {
      return proto;
    }

    /**
     * Converts a protobuf aggregation phase to this enum.
     *
     * @param proto the protobuf representation
     * @return the corresponding enum value
     */
    public static AggregationPhase fromProto(io.substrait.proto.AggregationPhase proto) {
      for (AggregationPhase v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }

      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  /** Defines the sort direction and null handling for sort operations. */
  enum SortDirection {
    /** Ascending sort with nulls appearing first. */
    ASC_NULLS_FIRST(io.substrait.proto.SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_FIRST),
    /** Ascending sort with nulls appearing last. */
    ASC_NULLS_LAST(io.substrait.proto.SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_LAST),
    /** Descending sort with nulls appearing first. */
    DESC_NULLS_FIRST(io.substrait.proto.SortField.SortDirection.SORT_DIRECTION_DESC_NULLS_FIRST),
    /** Descending sort with nulls appearing last. */
    DESC_NULLS_LAST(io.substrait.proto.SortField.SortDirection.SORT_DIRECTION_DESC_NULLS_LAST),
    /** Clustered sort (implementation-defined ordering). */
    CLUSTERED(io.substrait.proto.SortField.SortDirection.SORT_DIRECTION_CLUSTERED);

    private final io.substrait.proto.SortField.SortDirection proto;

    SortDirection(io.substrait.proto.SortField.SortDirection proto) {
      this.proto = proto;
    }

    /**
     * Converts this sort direction to its protobuf representation.
     *
     * @return the protobuf representation
     */
    public io.substrait.proto.SortField.SortDirection toProto() {
      return proto;
    }

    /**
     * Converts a protobuf sort direction to this enum.
     *
     * @param proto the protobuf representation
     * @return the corresponding enum value
     */
    public static SortDirection fromProto(io.substrait.proto.SortField.SortDirection proto) {
      for (SortDirection v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }

      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  /** Defines the behavior when a cast operation fails. */
  enum FailureBehavior {
    /** Unspecified failure behavior. */
    UNSPECIFIED(io.substrait.proto.Expression.Cast.FailureBehavior.FAILURE_BEHAVIOR_UNSPECIFIED),
    /** Return null on cast failure. */
    RETURN_NULL(io.substrait.proto.Expression.Cast.FailureBehavior.FAILURE_BEHAVIOR_RETURN_NULL),
    /** Throw an exception on cast failure. */
    THROW_EXCEPTION(
        io.substrait.proto.Expression.Cast.FailureBehavior.FAILURE_BEHAVIOR_THROW_EXCEPTION);

    private final io.substrait.proto.Expression.Cast.FailureBehavior proto;

    FailureBehavior(io.substrait.proto.Expression.Cast.FailureBehavior proto) {
      this.proto = proto;
    }

    /**
     * Converts this failure behavior to its protobuf representation.
     *
     * @return the protobuf representation
     */
    public io.substrait.proto.Expression.Cast.FailureBehavior toProto() {
      return proto;
    }

    /**
     * Converts a protobuf failure behavior to this enum.
     *
     * @param proto the protobuf representation
     * @return the corresponding enum value
     */
    public static FailureBehavior fromProto(
        io.substrait.proto.Expression.Cast.FailureBehavior proto) {
      for (FailureBehavior v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }

      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }
}
