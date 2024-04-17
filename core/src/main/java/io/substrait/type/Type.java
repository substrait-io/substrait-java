package io.substrait.type;

import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension;
import io.substrait.function.NullableType;
import io.substrait.function.ParameterizedType;
import io.substrait.function.TypeExpression;
import org.immutables.value.Value;

@Value.Enclosing
public interface Type extends TypeExpression, ParameterizedType, NullableType, FunctionArg {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Type.class);

  public static TypeCreator withNullability(boolean nullable) {
    return nullable ? TypeCreator.NULLABLE : TypeCreator.REQUIRED;
  }

  <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E;

  @Override
  default <R, E extends Throwable> R accept(
      SimpleExtension.Function fnDef, int argIdx, FuncArgVisitor<R, E> fnArgVisitor) throws E {
    return fnArgVisitor.visitType(fnDef, argIdx, this);
  }

  @Value.Immutable
  abstract static class Bool implements Type {
    public static ImmutableType.Bool.Builder builder() {
      return ImmutableType.Bool.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class I8 implements Type {
    public static ImmutableType.I8.Builder builder() {
      return ImmutableType.I8.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class I16 implements Type {
    public static ImmutableType.I16.Builder builder() {
      return ImmutableType.I16.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class I32 implements Type {
    public static ImmutableType.I32.Builder builder() {
      return ImmutableType.I32.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class I64 implements Type {
    public static ImmutableType.I64.Builder builder() {
      return ImmutableType.I64.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class FP32 implements Type {
    public static ImmutableType.FP32.Builder builder() {
      return ImmutableType.FP32.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class FP64 implements Type {
    public static ImmutableType.FP64.Builder builder() {
      return ImmutableType.FP64.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class Str implements Type {
    public static ImmutableType.Str.Builder builder() {
      return ImmutableType.Str.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class Binary implements Type {
    public static ImmutableType.Binary.Builder builder() {
      return ImmutableType.Binary.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class Date implements Type {
    public static ImmutableType.Date.Builder builder() {
      return ImmutableType.Date.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class Time implements Type {
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
  abstract static class TimestampTZ implements Type {

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
  abstract static class Timestamp implements Type {

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
  abstract static class IntervalYear implements Type {
    public static ImmutableType.IntervalYear.Builder builder() {
      return ImmutableType.IntervalYear.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class IntervalDay implements Type {
    public static ImmutableType.IntervalDay.Builder builder() {
      return ImmutableType.IntervalDay.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class UUID implements Type {
    public static ImmutableType.UUID.Builder builder() {
      return ImmutableType.UUID.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class FixedChar implements Type {
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
  abstract static class VarChar implements Type {
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
  abstract static class FixedBinary implements Type {
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
  abstract static class Decimal implements Type {
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
  abstract static class PrecisionTimestamp implements Type {
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
  abstract static class PrecisionTimestampTZ implements Type {
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
  abstract static class Struct implements Type {
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
  abstract static class ListType implements Type {
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
  abstract static class Map implements Type {
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
  abstract static class UserDefined implements Type {

    public abstract String uri();

    public abstract String name();

    public static ImmutableType.UserDefined.Builder builder() {
      return ImmutableType.UserDefined.builder();
    }

    @Override
    public <R, E extends Throwable> R accept(TypeVisitor<R, E> typeVisitor) throws E {
      return typeVisitor.visit(this);
    }
  }
}
