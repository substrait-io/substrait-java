package io.substrait.function;

import io.substrait.type.TypeVisitor;
import org.immutables.value.Value;

@Value.Enclosing
public interface TypeExpression {

  static class RequiredTypeExpressionVisitorException extends RuntimeException {}

  <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E;

  public static TypeExpressionCreator withNullability(boolean nullable) {
    return nullable ? TypeExpressionCreator.NULLABLE : TypeExpressionCreator.REQUIRED;
  }

  abstract static class BaseTypeExpression implements TypeExpression {
    public final <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      if (typeVisitor instanceof TypeExpressionVisitor) {
        return acceptE((TypeExpressionVisitor<R, E>) typeVisitor);
      }
      throw new RequiredTypeExpressionVisitorException();
    }

    abstract <R, E extends Throwable> R acceptE(
        final TypeExpressionVisitor<R, E> parameterizedTypeVisitor) throws E;
  }

  @Value.Immutable
  abstract static class FixedChar extends BaseTypeExpression implements NullableType {
    public abstract TypeExpression length();

    public static ImmutableTypeExpression.FixedChar.Builder builder() {
      return ImmutableTypeExpression.FixedChar.builder();
    }

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class VarChar extends BaseTypeExpression implements NullableType {
    public abstract TypeExpression length();

    public static ImmutableTypeExpression.VarChar.Builder builder() {
      return ImmutableTypeExpression.VarChar.builder();
    }

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class FixedBinary extends BaseTypeExpression implements NullableType {
    public abstract TypeExpression length();

    public static ImmutableTypeExpression.FixedBinary.Builder builder() {
      return ImmutableTypeExpression.FixedBinary.builder();
    }

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class Decimal extends BaseTypeExpression implements NullableType {
    public abstract TypeExpression scale();

    public abstract TypeExpression precision();

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }

    public static ImmutableTypeExpression.Decimal.Builder builder() {
      return ImmutableTypeExpression.Decimal.builder();
    }
  }

  @Value.Immutable
  abstract static class IntervalDay extends BaseTypeExpression implements NullableType {

    public abstract TypeExpression precision();

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }

    public static ImmutableTypeExpression.IntervalDay.Builder builder() {
      return ImmutableTypeExpression.IntervalDay.builder();
    }
  }

  @Value.Immutable
  abstract static class IntervalCompound extends BaseTypeExpression implements NullableType {

    public abstract TypeExpression precision();

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }

    public static ImmutableTypeExpression.IntervalCompound.Builder builder() {
      return ImmutableTypeExpression.IntervalCompound.builder();
    }
  }

  @Value.Immutable
  abstract static class PrecisionTimestamp extends BaseTypeExpression implements NullableType {

    public abstract TypeExpression precision();

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }

    public static ImmutableTypeExpression.PrecisionTimestamp.Builder builder() {
      return ImmutableTypeExpression.PrecisionTimestamp.builder();
    }
  }

  @Value.Immutable
  abstract static class PrecisionTimestampTZ extends BaseTypeExpression implements NullableType {

    public abstract TypeExpression precision();

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }

    public static ImmutableTypeExpression.PrecisionTimestampTZ.Builder builder() {
      return ImmutableTypeExpression.PrecisionTimestampTZ.builder();
    }
  }

  @Value.Immutable
  abstract static class Struct extends BaseTypeExpression implements NullableType {
    public abstract java.util.List<TypeExpression> fields();

    public static ImmutableTypeExpression.Struct.Builder builder() {
      return ImmutableTypeExpression.Struct.builder();
    }

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class ListType extends BaseTypeExpression implements NullableType {
    public abstract TypeExpression elementType();

    public static ImmutableTypeExpression.ListType.Builder builder() {
      return ImmutableTypeExpression.ListType.builder();
    }

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class Map extends BaseTypeExpression implements NullableType {
    public abstract TypeExpression key();

    public abstract TypeExpression value();

    public static ImmutableTypeExpression.Map.Builder builder() {
      return ImmutableTypeExpression.Map.builder();
    }

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class BinaryOperation extends BaseTypeExpression {
    public enum OpType {
      ADD,
      SUBTRACT,
      MULTIPLY,
      DIVIDE,
      MIN,
      MAX,
      LT,
      GT,
      LTE,
      GTE,
      AND,
      OR,
      EQ,
      NOT_EQ,
      COVERS
    }

    public abstract OpType opType();

    public abstract TypeExpression left();

    public abstract TypeExpression right();

    public static ImmutableTypeExpression.BinaryOperation.Builder builder() {
      return ImmutableTypeExpression.BinaryOperation.builder();
    }

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class NotOperation extends BaseTypeExpression {
    public abstract TypeExpression inner();

    public static ImmutableTypeExpression.NotOperation.Builder builder() {
      return ImmutableTypeExpression.NotOperation.builder();
    }

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class IfOperation extends BaseTypeExpression {
    public abstract TypeExpression ifCondition();

    public abstract TypeExpression thenExpr();

    public abstract TypeExpression elseExpr();

    public static ImmutableTypeExpression.IfOperation.Builder builder() {
      return ImmutableTypeExpression.IfOperation.builder();
    }

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class IntegerLiteral extends BaseTypeExpression {
    public abstract int value();

    public static ImmutableTypeExpression.IntegerLiteral.Builder builder() {
      return ImmutableTypeExpression.IntegerLiteral.builder();
    }

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class ReturnProgram extends BaseTypeExpression {
    public abstract java.util.List<Assignment> assignments();

    public abstract TypeExpression finalExpression();

    @Value.Immutable
    public abstract static class Assignment {
      public abstract java.lang.String name();

      public abstract TypeExpression expr();

      public static ImmutableTypeExpression.Assignment.Builder builder() {
        return ImmutableTypeExpression.Assignment.builder();
      }
    }

    public static ImmutableTypeExpression.ReturnProgram.Builder builder() {
      return ImmutableTypeExpression.ReturnProgram.builder();
    }

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }
}
