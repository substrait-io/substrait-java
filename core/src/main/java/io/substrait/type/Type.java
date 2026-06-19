package io.substrait.type;

import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension;
import io.substrait.function.NullableType;
import io.substrait.function.ParameterizedType;
import io.substrait.function.TypeExpression;
import io.substrait.util.VisitationContext;
import org.immutables.value.Value;

/** The Substrait type model: a concrete, fully-resolved data type. */
@Value.Enclosing
public interface Type extends TypeExpression, ParameterizedType, NullableType, FunctionArg {

  /**
   * Returns a copy of the {@link Type} with the specified nullability.
   *
   * <p>This method is implemented by all concrete {@link Type} classes via Immutables code
   * generation.
   *
   * @param nullable the desired nullability
   * @return a copy of this type with the given nullability
   */
  Type withNullable(boolean nullable);

  /**
   * Returns the type creator for the given nullability.
   *
   * @param nullable whether nullable types are desired
   * @return the matching type creator
   */
  static TypeCreator withNullability(boolean nullable) {
    return nullable ? TypeCreator.NULLABLE : TypeCreator.REQUIRED;
  }

  @Override
  <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E;

  @Override
  default <R, C extends VisitationContext, E extends Throwable> R accept(
      SimpleExtension.Function fnDef, int argIdx, FuncArgVisitor<R, C, E> fnArgVisitor, C context)
      throws E {
    return fnArgVisitor.visitType(fnDef, argIdx, this, context);
  }

  /**
   * Compares this type with another type, ignoring nullability differences.
   *
   * @param other the type to compare with
   * @return true if the types are equal when both are treated as nullable
   */
  default boolean equalsIgnoringNullability(Type other) {
    return TypeCreator.asNullable(this).equals(TypeCreator.asNullable(other));
  }

  /** The boolean type. */
  @Value.Immutable
  abstract class Bool implements Type {
    /**
     * Creates a builder for {@link Bool}.
     *
     * @return a new builder
     */
    public static ImmutableType.Bool.Builder builder() {
      return ImmutableType.Bool.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** The 8-bit integer type. */
  @Value.Immutable
  abstract class I8 implements Type {
    /**
     * Creates a builder for {@link I8}.
     *
     * @return a new builder
     */
    public static ImmutableType.I8.Builder builder() {
      return ImmutableType.I8.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** The 16-bit integer type. */
  @Value.Immutable
  abstract class I16 implements Type {
    /**
     * Creates a builder for {@link I16}.
     *
     * @return a new builder
     */
    public static ImmutableType.I16.Builder builder() {
      return ImmutableType.I16.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** The 32-bit integer type. */
  @Value.Immutable
  abstract class I32 implements Type {
    /**
     * Creates a builder for {@link I32}.
     *
     * @return a new builder
     */
    public static ImmutableType.I32.Builder builder() {
      return ImmutableType.I32.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** The 64-bit integer type. */
  @Value.Immutable
  abstract class I64 implements Type {
    /**
     * Creates a builder for {@link I64}.
     *
     * @return a new builder
     */
    public static ImmutableType.I64.Builder builder() {
      return ImmutableType.I64.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** The 32-bit floating point type. */
  @Value.Immutable
  abstract class FP32 implements Type {
    /**
     * Creates a builder for {@link FP32}.
     *
     * @return a new builder
     */
    public static ImmutableType.FP32.Builder builder() {
      return ImmutableType.FP32.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** The 64-bit floating point type. */
  @Value.Immutable
  abstract class FP64 implements Type {
    /**
     * Creates a builder for {@link FP64}.
     *
     * @return a new builder
     */
    public static ImmutableType.FP64.Builder builder() {
      return ImmutableType.FP64.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** The string type. */
  @Value.Immutable
  abstract class Str implements Type {
    /**
     * Creates a builder for {@link Str}.
     *
     * @return a new builder
     */
    public static ImmutableType.Str.Builder builder() {
      return ImmutableType.Str.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** The binary type. */
  @Value.Immutable
  abstract class Binary implements Type {
    /**
     * Creates a builder for {@link Binary}.
     *
     * @return a new builder
     */
    public static ImmutableType.Binary.Builder builder() {
      return ImmutableType.Binary.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** The date type. */
  @Value.Immutable
  abstract class Date implements Type {
    /**
     * Creates a builder for {@link Date}.
     *
     * @return a new builder
     */
    public static ImmutableType.Date.Builder builder() {
      return ImmutableType.Date.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /**
   * The time type.
   *
   * @deprecated use {@link PrecisionTime} instead
   */
  @Value.Immutable
  @Deprecated
  abstract class Time implements Type {
    /**
     * Creates a builder for {@link Time}.
     *
     * @return a new builder
     */
    public static ImmutableType.Time.Builder builder() {
      return ImmutableType.Time.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** Deprecated, use {@link PrecisionTimestampTZ} instead */
  @Value.Immutable
  @Deprecated
  abstract class TimestampTZ implements Type {

    /**
     * Creates a builder for {@link TimestampTZ}.
     *
     * @return a new builder
     * @deprecated use {@link PrecisionTimestampTZ#builder()} instead
     */
    @Deprecated
    public static ImmutableType.TimestampTZ.Builder builder() {
      return ImmutableType.TimestampTZ.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** Deprecated, use {@link PrecisionTimestamp} instead */
  @Value.Immutable
  @Deprecated
  abstract class Timestamp implements Type {

    /**
     * Creates a builder for {@link Timestamp}.
     *
     * @return a new builder
     * @deprecated use {@link PrecisionTimestamp#builder()} instead
     */
    @Deprecated
    public static ImmutableType.Timestamp.Builder builder() {
      return ImmutableType.Timestamp.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** The year-month interval type. */
  @Value.Immutable
  abstract class IntervalYear implements Type {
    /**
     * Creates a builder for {@link IntervalYear}.
     *
     * @return a new builder
     */
    public static ImmutableType.IntervalYear.Builder builder() {
      return ImmutableType.IntervalYear.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** The day-time interval type. */
  @Value.Immutable
  abstract class IntervalDay implements Type {
    /**
     * Returns the fractional-second precision.
     *
     * @return the precision
     */
    public abstract int precision();

    /**
     * Creates a builder for {@link IntervalDay}.
     *
     * @return a new builder
     */
    public static ImmutableType.IntervalDay.Builder builder() {
      return ImmutableType.IntervalDay.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** The compound interval type. */
  @Value.Immutable
  abstract class IntervalCompound implements Type {
    /**
     * Returns the fractional-second precision.
     *
     * @return the precision
     */
    public abstract int precision();

    /**
     * Creates a builder for {@link IntervalCompound}.
     *
     * @return a new builder
     */
    public static ImmutableType.IntervalCompound.Builder builder() {
      return ImmutableType.IntervalCompound.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** The UUID type. */
  @Value.Immutable
  abstract class UUID implements Type {
    /**
     * Creates a builder for {@link UUID}.
     *
     * @return a new builder
     */
    public static ImmutableType.UUID.Builder builder() {
      return ImmutableType.UUID.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** The fixed-length character type. */
  @Value.Immutable
  abstract class FixedChar implements Type {
    /**
     * Returns the fixed length.
     *
     * @return the length
     */
    public abstract int length();

    /**
     * Creates a builder for {@link FixedChar}.
     *
     * @return a new builder
     */
    public static ImmutableType.FixedChar.Builder builder() {
      return ImmutableType.FixedChar.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** The variable-length character type. */
  @Value.Immutable
  abstract class VarChar implements Type {
    /**
     * Returns the maximum length.
     *
     * @return the maximum length
     */
    public abstract int length();

    /**
     * Creates a builder for {@link VarChar}.
     *
     * @return a new builder
     */
    public static ImmutableType.VarChar.Builder builder() {
      return ImmutableType.VarChar.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** The fixed-length binary type. */
  @Value.Immutable
  abstract class FixedBinary implements Type {
    /**
     * Returns the fixed length.
     *
     * @return the length
     */
    public abstract int length();

    /**
     * Creates a builder for {@link FixedBinary}.
     *
     * @return a new builder
     */
    public static ImmutableType.FixedBinary.Builder builder() {
      return ImmutableType.FixedBinary.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** The decimal type. */
  @Value.Immutable
  abstract class Decimal implements Type {
    /**
     * Returns the number of digits to the right of the decimal point.
     *
     * @return the scale
     */
    public abstract int scale();

    /**
     * Returns the total number of digits.
     *
     * @return the precision
     */
    public abstract int precision();

    /**
     * Creates a builder for {@link Decimal}.
     *
     * @return a new builder
     */
    public static ImmutableType.Decimal.Builder builder() {
      return ImmutableType.Decimal.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** The precision-time type. */
  @Value.Immutable
  abstract class PrecisionTime implements Type {
    /**
     * Returns the fractional-second precision.
     *
     * @return the precision
     */
    public abstract int precision();

    /**
     * Creates a builder for {@link PrecisionTime}.
     *
     * @return a new builder
     */
    public static ImmutableType.PrecisionTime.Builder builder() {
      return ImmutableType.PrecisionTime.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** The precision-timestamp (without timezone) type. */
  @Value.Immutable
  abstract class PrecisionTimestamp implements Type {
    /**
     * Returns the fractional-second precision.
     *
     * @return the precision
     */
    public abstract int precision();

    /**
     * Creates a builder for {@link PrecisionTimestamp}.
     *
     * @return a new builder
     */
    public static ImmutableType.PrecisionTimestamp.Builder builder() {
      return ImmutableType.PrecisionTimestamp.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** The precision-timestamp with timezone type. */
  @Value.Immutable
  abstract class PrecisionTimestampTZ implements Type {
    /**
     * Returns the fractional-second precision.
     *
     * @return the precision
     */
    public abstract int precision();

    /**
     * Creates a builder for {@link PrecisionTimestampTZ}.
     *
     * @return a new builder
     */
    public static ImmutableType.PrecisionTimestampTZ.Builder builder() {
      return ImmutableType.PrecisionTimestampTZ.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** The function type. */
  @Value.Immutable
  abstract class Func implements Type {
    /**
     * Returns the parameter types.
     *
     * @return the parameter types
     */
    public abstract java.util.List<Type> parameterTypes();

    /**
     * Returns the return type.
     *
     * @return the return type
     */
    public abstract Type returnType();

    /**
     * Creates a builder for {@link Func}.
     *
     * @return a new builder
     */
    public static ImmutableType.Func.Builder builder() {
      return ImmutableType.Func.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** The struct type. */
  @Value.Immutable
  abstract class Struct implements Type {
    /**
     * Returns the field types.
     *
     * @return the field types
     */
    public abstract java.util.List<Type> fields();

    /**
     * Creates a builder for {@link Struct}.
     *
     * @return a new builder
     */
    public static ImmutableType.Struct.Builder builder() {
      return ImmutableType.Struct.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** The list type. */
  @Value.Immutable
  abstract class ListType implements Type {
    /**
     * Returns the element type.
     *
     * @return the element type
     */
    public abstract Type elementType();

    /**
     * Creates a builder for {@link ListType}.
     *
     * @return a new builder
     */
    public static ImmutableType.ListType.Builder builder() {
      return ImmutableType.ListType.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** The map type. */
  @Value.Immutable
  abstract class Map implements Type {
    /**
     * Returns the key type.
     *
     * @return the key type
     */
    public abstract Type key();

    /**
     * Returns the value type.
     *
     * @return the value type
     */
    public abstract Type value();

    /**
     * Creates a builder for {@link Map}.
     *
     * @return a new builder
     */
    public static ImmutableType.Map.Builder builder() {
      return ImmutableType.Map.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /** A user-defined (extension) type, identified by an extension URN and name. */
  @Value.Immutable
  interface UserDefined extends Type {

    /**
     * Returns the extension URN declaring this type.
     *
     * @return the extension URN
     */
    String urn();

    /**
     * Returns the name of this type within its extension.
     *
     * @return the type name
     */
    String name();

    /**
     * Returns the type parameters for this user-defined type.
     *
     * <p>Type parameters are used to represent parameterized/generic types, such as {@code
     * vector<i32>}.
     *
     * @return a list of type parameters, or an empty list if this type is not parameterized
     */
    @Value.Default
    default java.util.List<Parameter> typeParameters() {
      return java.util.Collections.emptyList();
    }

    /**
     * Returns the type variation reference for this user-defined type.
     *
     * <p>Type variations allow different physical representations or semantics for the same logical
     * type. The reference value maps to an {@code ExtensionTypeVariation} declaration in the plan.
     *
     * @return the type variation reference, or {@code 0} if using the default variation
     */
    @Value.Default
    default int typeVariationReference() {
      return 0;
    }

    /**
     * Creates a builder for {@link UserDefined}.
     *
     * @return a new builder
     */
    static ImmutableType.UserDefined.Builder builder() {
      return ImmutableType.UserDefined.builder();
    }

    @Override
    default <R, E extends Throwable> R accept(TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  /**
   * Represents a type parameter for user-defined types.
   *
   * <p>Type parameters can be data types (like {@code i32} in {@code List<i32>}), or value
   * parameters (like the {@code 10} in {@code VARCHAR<10>}). This interface provides a type-safe
   * representation of all possible parameter kinds.
   */
  interface Parameter {}

  /** A data type parameter, such as the {@code i32} in {@code List<i32>}. */
  @Value.Immutable
  abstract class ParameterDataType implements Parameter {
    /**
     * Returns the data type used as the parameter.
     *
     * @return the parameter type
     */
    public abstract Type type();
  }

  /** A boolean value parameter. */
  @Value.Immutable
  abstract class ParameterBooleanValue implements Parameter {
    /**
     * Returns the boolean parameter value.
     *
     * @return the value
     */
    public abstract boolean value();
  }

  /** An integer value parameter, such as the {@code 10} in {@code VARCHAR<10>}. */
  @Value.Immutable
  abstract class ParameterIntegerValue implements Parameter {
    /**
     * Returns the integer parameter value.
     *
     * @return the value
     */
    public abstract long value();
  }

  /** An enum value parameter (represented as a string). */
  @Value.Immutable
  abstract class ParameterEnumValue implements Parameter {
    /**
     * Returns the enum parameter value.
     *
     * @return the value
     */
    public abstract String value();
  }

  /** A string value parameter. */
  @Value.Immutable
  abstract class ParameterStringValue implements Parameter {
    /**
     * Returns the string parameter value.
     *
     * @return the value
     */
    public abstract String value();
  }

  /** An explicitly null/unspecified parameter, used to select the default value (if any). */
  class ParameterNull implements Parameter {
    /** The shared singleton instance of the null parameter. */
    public static final ParameterNull INSTANCE = new ParameterNull();

    private ParameterNull() {}
  }
}
