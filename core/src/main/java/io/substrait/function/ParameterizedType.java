package io.substrait.function;

import io.substrait.type.TypeVisitor;
import java.util.Locale;
import org.immutables.value.Value;

/**
 * Types used in function argument declarations. Can utilize strings for integer or type parameters.
 */
@Value.Enclosing
public interface ParameterizedType extends TypeExpression {

  class RequiredParameterizedVisitorException extends RuntimeException {
    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  }

  <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E;

  static ParameterizedTypeCreator withNullability(boolean nullable) {
    return nullable ? ParameterizedTypeCreator.NULLABLE : ParameterizedTypeCreator.REQUIRED;
  }

  interface NullableParameterizedType extends ParameterizedType {
    boolean nullable();
  }

  default boolean isWildcard() {
    return false;
  }

  abstract class BaseParameterizedType implements ParameterizedType {
    public final <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      if (typeVisitor instanceof ParameterizedTypeVisitor) {
        return accept((ParameterizedTypeVisitor<R, E>) typeVisitor);
      }
      throw new RequiredParameterizedVisitorException();
    }

    abstract <R, E extends Throwable> R accept(
        final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor) throws E;
  }

  @Value.Immutable
  abstract class FixedChar extends BaseParameterizedType implements NullableType {
    public abstract StringLiteral length();

    public static ImmutableParameterizedType.FixedChar.Builder builder() {
      return ImmutableParameterizedType.FixedChar.builder();
    }

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class VarChar extends BaseParameterizedType implements NullableType {
    public abstract StringLiteral length();

    public static ImmutableParameterizedType.VarChar.Builder builder() {
      return ImmutableParameterizedType.VarChar.builder();
    }

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class FixedBinary extends BaseParameterizedType implements NullableType {
    public abstract StringLiteral length();

    public static ImmutableParameterizedType.FixedBinary.Builder builder() {
      return ImmutableParameterizedType.FixedBinary.builder();
    }

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class Decimal extends BaseParameterizedType implements NullableType {
    public abstract StringLiteral scale();

    public abstract StringLiteral precision();

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }

    public static ImmutableParameterizedType.Decimal.Builder builder() {
      return ImmutableParameterizedType.Decimal.builder();
    }
  }

  @Value.Immutable
  abstract class IntervalDay extends BaseParameterizedType implements NullableType {
    public abstract StringLiteral precision();

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }

    public static ImmutableParameterizedType.IntervalDay.Builder builder() {
      return ImmutableParameterizedType.IntervalDay.builder();
    }
  }

  @Value.Immutable
  abstract class IntervalCompound extends BaseParameterizedType implements NullableType {
    public abstract StringLiteral precision();

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }

    public static ImmutableParameterizedType.IntervalCompound.Builder builder() {
      return ImmutableParameterizedType.IntervalCompound.builder();
    }
  }

  @Value.Immutable
  abstract class PrecisionTime extends BaseParameterizedType implements NullableType {
    public abstract StringLiteral precision();

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }

    public static ImmutableParameterizedType.PrecisionTime.Builder builder() {
      return ImmutableParameterizedType.PrecisionTime.builder();
    }
  }

  @Value.Immutable
  abstract class PrecisionTimestamp extends BaseParameterizedType implements NullableType {
    public abstract StringLiteral precision();

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }

    public static ImmutableParameterizedType.PrecisionTimestamp.Builder builder() {
      return ImmutableParameterizedType.PrecisionTimestamp.builder();
    }
  }

  @Value.Immutable
  abstract class PrecisionTimestampTZ extends BaseParameterizedType implements NullableType {
    public abstract StringLiteral precision();

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }

    public static ImmutableParameterizedType.PrecisionTimestampTZ.Builder builder() {
      return ImmutableParameterizedType.PrecisionTimestampTZ.builder();
    }
  }

  @Value.Immutable
  abstract class Struct extends BaseParameterizedType implements NullableType {
    public abstract java.util.List<ParameterizedType> fields();

    public static ImmutableParameterizedType.Struct.Builder builder() {
      return ImmutableParameterizedType.Struct.builder();
    }

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class ListType extends BaseParameterizedType implements NullableType {
    public abstract ParameterizedType name();

    public static ImmutableParameterizedType.ListType.Builder builder() {
      return ImmutableParameterizedType.ListType.builder();
    }

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class Map extends BaseParameterizedType implements NullableType {
    public abstract ParameterizedType key();

    public abstract ParameterizedType value();

    public static ImmutableParameterizedType.Map.Builder builder() {
      return ImmutableParameterizedType.Map.builder();
    }

    @Override
    <R, E extends Throwable> R accept(final ParameterizedTypeVisitor<R, E> parameterizedTypeVisitor)
        throws E {
      return parameterizedTypeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract class StringLiteral extends BaseParameterizedType implements NullableType {
    public abstract String value();

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
