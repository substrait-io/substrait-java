package io.substrait.type;

import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension;
import io.substrait.function.NullableType;
import io.substrait.function.ParameterizedType;
import io.substrait.function.TypeExpression;
import io.substrait.util.VisitationContext;
import org.immutables.value.Value;

@Value.Enclosing
public interface Type extends TypeExpression, ParameterizedType, NullableType, FunctionArg {

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

  @Value.Immutable
  abstract class Bool implements Type {
    public static ImmutableType.Bool.Builder builder() {
      return ImmutableType.Bool.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class I8 implements Type {
    public static ImmutableType.I8.Builder builder() {
      return ImmutableType.I8.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class I16 implements Type {
    public static ImmutableType.I16.Builder builder() {
      return ImmutableType.I16.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class I32 implements Type {
    public static ImmutableType.I32.Builder builder() {
      return ImmutableType.I32.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class I64 implements Type {
    public static ImmutableType.I64.Builder builder() {
      return ImmutableType.I64.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class FP32 implements Type {
    public static ImmutableType.FP32.Builder builder() {
      return ImmutableType.FP32.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class FP64 implements Type {
    public static ImmutableType.FP64.Builder builder() {
      return ImmutableType.FP64.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class Str implements Type {
    public static ImmutableType.Str.Builder builder() {
      return ImmutableType.Str.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class Binary implements Type {
    public static ImmutableType.Binary.Builder builder() {
      return ImmutableType.Binary.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class Date implements Type {
    public static ImmutableType.Date.Builder builder() {
      return ImmutableType.Date.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class Time implements Type {
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

    /** Deprecated, use {@link PrecisionTimestampTZ#builder()} instead */
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

    /** Deprecated, use {@link PrecisionTimestamp#builder()} instead */
    @Deprecated
    public static ImmutableType.Timestamp.Builder builder() {
      return ImmutableType.Timestamp.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class IntervalYear implements Type {
    public static ImmutableType.IntervalYear.Builder builder() {
      return ImmutableType.IntervalYear.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class IntervalDay implements Type {
    public abstract int precision();

    public static ImmutableType.IntervalDay.Builder builder() {
      return ImmutableType.IntervalDay.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class IntervalCompound implements Type {
    public abstract int precision();

    public static ImmutableType.IntervalCompound.Builder builder() {
      return ImmutableType.IntervalCompound.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class UUID implements Type {
    public static ImmutableType.UUID.Builder builder() {
      return ImmutableType.UUID.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class FixedChar implements Type {
    public abstract int length();

    public static ImmutableType.FixedChar.Builder builder() {
      return ImmutableType.FixedChar.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class VarChar implements Type {
    public abstract int length();

    public static ImmutableType.VarChar.Builder builder() {
      return ImmutableType.VarChar.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class FixedBinary implements Type {
    public abstract int length();

    public static ImmutableType.FixedBinary.Builder builder() {
      return ImmutableType.FixedBinary.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class Decimal implements Type {
    public abstract int scale();

    public abstract int precision();

    public static ImmutableType.Decimal.Builder builder() {
      return ImmutableType.Decimal.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class PrecisionTime implements Type {
    public abstract int precision();

    public static ImmutableType.PrecisionTime.Builder builder() {
      return ImmutableType.PrecisionTime.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class PrecisionTimestamp implements Type {
    public abstract int precision();

    public static ImmutableType.PrecisionTimestamp.Builder builder() {
      return ImmutableType.PrecisionTimestamp.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class PrecisionTimestampTZ implements Type {
    public abstract int precision();

    public static ImmutableType.PrecisionTimestampTZ.Builder builder() {
      return ImmutableType.PrecisionTimestampTZ.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class Struct implements Type {
    public abstract java.util.List<Type> fields();

    public static ImmutableType.Struct.Builder builder() {
      return ImmutableType.Struct.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class ListType implements Type {
    public abstract Type elementType();

    public static ImmutableType.ListType.Builder builder() {
      return ImmutableType.ListType.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class Map implements Type {
    public abstract Type key();

    public abstract Type value();

    public static ImmutableType.Map.Builder builder() {
      return ImmutableType.Map.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class UserDefined implements Type {

    public abstract String urn();

    public abstract String name();

    /**
     * Returns the type parameters for this user-defined type.
     *
     * <p>Type parameters are used to represent parameterized/generic types, such as {@code
     * vector<i32>} or custom types like {@code FixedArray<100>}. Each parameter in the list can be
     * either a type (like {@code i32}) or a value (like the integer {@code 100}).
     *
     * <p>Unlike built-in parameterized types ({@link Map}, {@link ListType}, {@link Decimal}),
     * which have fixed, known schemas with concrete typed fields, user-defined types have variable,
     * unknown schemas. This is why UserDefined uses a generic {@link Parameter} list that can hold
     * any mix of types or values, while other parameterized types use concrete fields like {@code
     * Type key()} or {@code int precision()}.
     *
     * <p>For example, a user-defined {@code vector} type parameterized by {@code i32} would have
     * one type parameter containing the {@code i32} type definition, while a {@code FixedArray}
     * type might take an integer parameter specifying its size.
     *
     * @return a list of type parameters, or an empty list if this type is not parameterized
     */
    @Value.Default
    public java.util.List<Parameter> typeParameters() {
      return java.util.Collections.emptyList();
    }

    public static ImmutableType.UserDefined.Builder builder() {
      return ImmutableType.UserDefined.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(TypeVisitor<R, E> typeVisitor) throws E {
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
    public abstract Type type();
  }

  /** A boolean value parameter. */
  @Value.Immutable
  abstract class ParameterBooleanValue implements Parameter {
    public abstract boolean value();
  }

  /** An integer value parameter, such as the {@code 10} in {@code VARCHAR<10>}. */
  @Value.Immutable
  abstract class ParameterIntegerValue implements Parameter {
    public abstract long value();
  }

  /** An enum value parameter (represented as a string). */
  @Value.Immutable
  abstract class ParameterEnumValue implements Parameter {
    public abstract String value();
  }

  /** A string value parameter. */
  @Value.Immutable
  abstract class ParameterStringValue implements Parameter {
    public abstract String value();
  }

  /** An explicitly null/unspecified parameter, used to select the default value (if any). */
  class ParameterNull implements Parameter {
    public static final ParameterNull INSTANCE = new ParameterNull();

    private ParameterNull() {}
  }
}
