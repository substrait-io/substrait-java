package io.substrait.function;

import io.substrait.type.TypeVisitor;
import java.util.Locale;
import org.immutables.value.Value;

/**
 * Types used in function argument declarations. Can utilize strings for integer or type parameters.
 */
@Value.Enclosing
public interface ParameterizedType extends TypeExpression {

  /**
   * Thrown when a {@link ParameterizedType} is visited by a {@link TypeVisitor} that is not a
   * {@link ParameterizedTypeVisitor}.
   */
  class RequiredParameterizedVisitorException extends RuntimeException {

    private static final long serialVersionUID = 5009974222890249956L;

    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  }

  @Override
  <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E;

  /**
   * Returns the parameterized-type creator for the given nullability.
   *
   * @param nullable whether nullable types are desired
   * @return the matching creator
   */
  static ParameterizedTypeCreator withNullability(boolean nullable) {
    return nullable ? ParameterizedTypeCreator.NULLABLE : ParameterizedTypeCreator.REQUIRED;
  }

  /** A {@link ParameterizedType} that carries nullability information. */
  interface NullableParameterizedType extends ParameterizedType {
    /**
     * Returns whether this type is nullable.
     *
     * @return {@code true} if the type accepts null values
     */
    boolean nullable();
  }

  /**
   * Returns whether this type is a wildcard (a placeholder matching any type).
   *
   * @return {@code true} if this type is a wildcard
   */
  default boolean isWildcard() {
    return false;
  }

  /** Base class for parameterized types that dispatch to a {@link ParameterizedTypeVisitor}. */
  abstract class BaseParameterizedType implements ParameterizedType {
    @Override
    public final <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      if (typeVisitor instanceof ParameterizedTypeVisitor) {
        return accept((ParameterizedTypeVisitor<R, E>) typeVisitor);
      }
      throw new RequiredParameterizedVisitorException();
    }

    abstract <R, E extends Throwable> R accept(
        final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor) throws E;
  }

  /** A fixed-length character type with a parameterized length. */
  @Value.Immutable
  abstract class FixedChar extends BaseParameterizedType implements NullableType {
    /**
     * Returns the parameterized length.
     *
     * @return the length parameter
     */
    public abstract StringLiteral length();

    /**
     * Creates a builder for {@link FixedChar}.
     *
     * @return a new builder
     */
    public static ImmutableParameterizedType.FixedChar.Builder builder() {
      return ImmutableParameterizedType.FixedChar.builder();
    }

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }
  }

  /** A variable-length character type with a parameterized length. */
  @Value.Immutable
  abstract class VarChar extends BaseParameterizedType implements NullableType {
    /**
     * Returns the parameterized length.
     *
     * @return the length parameter
     */
    public abstract StringLiteral length();

    /**
     * Creates a builder for {@link VarChar}.
     *
     * @return a new builder
     */
    public static ImmutableParameterizedType.VarChar.Builder builder() {
      return ImmutableParameterizedType.VarChar.builder();
    }

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }
  }

  /** A fixed-length binary type with a parameterized length. */
  @Value.Immutable
  abstract class FixedBinary extends BaseParameterizedType implements NullableType {
    /**
     * Returns the parameterized length.
     *
     * @return the length parameter
     */
    public abstract StringLiteral length();

    /**
     * Creates a builder for {@link FixedBinary}.
     *
     * @return a new builder
     */
    public static ImmutableParameterizedType.FixedBinary.Builder builder() {
      return ImmutableParameterizedType.FixedBinary.builder();
    }

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }
  }

  /** A decimal type with parameterized precision and scale. */
  @Value.Immutable
  abstract class Decimal extends BaseParameterizedType implements NullableType {
    /**
     * Returns the parameterized scale.
     *
     * @return the scale parameter
     */
    public abstract StringLiteral scale();

    /**
     * Returns the parameterized precision.
     *
     * @return the precision parameter
     */
    public abstract StringLiteral precision();

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }

    /**
     * Creates a builder for {@link Decimal}.
     *
     * @return a new builder
     */
    public static ImmutableParameterizedType.Decimal.Builder builder() {
      return ImmutableParameterizedType.Decimal.builder();
    }
  }

  /** A day-time interval type with a parameterized precision. */
  @Value.Immutable
  abstract class IntervalDay extends BaseParameterizedType implements NullableType {
    /**
     * Returns the parameterized precision.
     *
     * @return the precision parameter
     */
    public abstract StringLiteral precision();

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }

    /**
     * Creates a builder for {@link IntervalDay}.
     *
     * @return a new builder
     */
    public static ImmutableParameterizedType.IntervalDay.Builder builder() {
      return ImmutableParameterizedType.IntervalDay.builder();
    }
  }

  /** A compound interval type with a parameterized precision. */
  @Value.Immutable
  abstract class IntervalCompound extends BaseParameterizedType implements NullableType {
    /**
     * Returns the parameterized precision.
     *
     * @return the precision parameter
     */
    public abstract StringLiteral precision();

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }

    /**
     * Creates a builder for {@link IntervalCompound}.
     *
     * @return a new builder
     */
    public static ImmutableParameterizedType.IntervalCompound.Builder builder() {
      return ImmutableParameterizedType.IntervalCompound.builder();
    }
  }

  /** A precision-time type with a parameterized precision. */
  @Value.Immutable
  abstract class PrecisionTime extends BaseParameterizedType implements NullableType {
    /**
     * Returns the parameterized precision.
     *
     * @return the precision parameter
     */
    public abstract StringLiteral precision();

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }

    /**
     * Creates a builder for {@link PrecisionTime}.
     *
     * @return a new builder
     */
    public static ImmutableParameterizedType.PrecisionTime.Builder builder() {
      return ImmutableParameterizedType.PrecisionTime.builder();
    }
  }

  /** A precision-timestamp (without timezone) type with a parameterized precision. */
  @Value.Immutable
  abstract class PrecisionTimestamp extends BaseParameterizedType implements NullableType {
    /**
     * Returns the parameterized precision.
     *
     * @return the precision parameter
     */
    public abstract StringLiteral precision();

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }

    /**
     * Creates a builder for {@link PrecisionTimestamp}.
     *
     * @return a new builder
     */
    public static ImmutableParameterizedType.PrecisionTimestamp.Builder builder() {
      return ImmutableParameterizedType.PrecisionTimestamp.builder();
    }
  }

  /** A precision-timestamp with timezone type with a parameterized precision. */
  @Value.Immutable
  abstract class PrecisionTimestampTZ extends BaseParameterizedType implements NullableType {
    /**
     * Returns the parameterized precision.
     *
     * @return the precision parameter
     */
    public abstract StringLiteral precision();

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }

    /**
     * Creates a builder for {@link PrecisionTimestampTZ}.
     *
     * @return a new builder
     */
    public static ImmutableParameterizedType.PrecisionTimestampTZ.Builder builder() {
      return ImmutableParameterizedType.PrecisionTimestampTZ.builder();
    }
  }

  /** A struct type whose fields are themselves parameterized types. */
  @Value.Immutable
  abstract class Struct extends BaseParameterizedType implements NullableType {
    /**
     * Returns the field types.
     *
     * @return the field types
     */
    public abstract java.util.List<ParameterizedType> fields();

    /**
     * Creates a builder for {@link Struct}.
     *
     * @return a new builder
     */
    public static ImmutableParameterizedType.Struct.Builder builder() {
      return ImmutableParameterizedType.Struct.builder();
    }

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }
  }

  /** A function type whose parameter and return types are parameterized types. */
  @Value.Immutable
  abstract class Func extends BaseParameterizedType implements NullableType {
    /**
     * Returns the parameter types.
     *
     * @return the parameter types
     */
    public abstract java.util.List<ParameterizedType> parameterTypes();

    /**
     * Returns the return type.
     *
     * @return the return type
     */
    public abstract ParameterizedType returnType();

    /**
     * Creates a builder for {@link Func}.
     *
     * @return a new builder
     */
    public static ImmutableParameterizedType.Func.Builder builder() {
      return ImmutableParameterizedType.Func.builder();
    }

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }
  }

  /** A list type whose element type is a parameterized type. */
  @Value.Immutable
  abstract class ListType extends BaseParameterizedType implements NullableType {
    /**
     * Returns the element type.
     *
     * @return the element type
     */
    public abstract ParameterizedType name();

    /**
     * Creates a builder for {@link ListType}.
     *
     * @return a new builder
     */
    public static ImmutableParameterizedType.ListType.Builder builder() {
      return ImmutableParameterizedType.ListType.builder();
    }

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }
  }

  /** A map type whose key and value types are parameterized types. */
  @Value.Immutable
  abstract class Map extends BaseParameterizedType implements NullableType {
    /**
     * Returns the key type.
     *
     * @return the key type
     */
    public abstract ParameterizedType key();

    /**
     * Returns the value type.
     *
     * @return the value type
     */
    public abstract ParameterizedType value();

    /**
     * Creates a builder for {@link Map}.
     *
     * @return a new builder
     */
    public static ImmutableParameterizedType.Map.Builder builder() {
      return ImmutableParameterizedType.Map.builder();
    }

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }
  }

  /** A string-literal type parameter, used as a placeholder for an integer or type parameter. */
  @Value.Immutable
  abstract class StringLiteral extends BaseParameterizedType implements NullableType {
    /**
     * Returns the literal parameter value.
     *
     * @return the parameter value
     */
    public abstract String value();

    /**
     * Creates a builder for {@link StringLiteral}.
     *
     * @return a new builder
     */
    public static ImmutableParameterizedType.StringLiteral.Builder builder() {
      return ImmutableParameterizedType.StringLiteral.builder();
    }

    @Override
    public boolean isWildcard() {
      return value().toLowerCase(Locale.ROOT).startsWith("any");
    }

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }
  }
}
