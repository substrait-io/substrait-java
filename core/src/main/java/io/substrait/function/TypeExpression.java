package io.substrait.function;

import io.substrait.type.TypeVisitor;
import org.immutables.value.Value;

/**
 * A type derivation expression, used to describe how a function's output type is computed from its
 * argument types.
 */
@Value.Enclosing
public interface TypeExpression {

  /**
   * Thrown when a {@link TypeExpression} is visited by a {@link TypeVisitor} that is not a {@link
   * TypeExpressionVisitor}.
   */
  class RequiredTypeExpressionVisitorException extends RuntimeException {
    private static final long serialVersionUID = 8381558691397737963L;
  }

  /**
   * Accepts a type visitor.
   *
   * @param <R> the visitor result type
   * @param <E> the exception type that may be thrown
   * @param typeVisitor the visitor
   * @return the visit result
   * @throws E if the visit fails
   */
  <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E;

  /**
   * Returns the type-expression creator for the given nullability.
   *
   * @param nullable whether nullable type expressions are desired
   * @return the matching creator
   */
  static TypeExpressionCreator withNullability(boolean nullable) {
    return nullable ? TypeExpressionCreator.NULLABLE : TypeExpressionCreator.REQUIRED;
  }

  /** Base class for type expressions that dispatch to a {@link TypeExpressionVisitor}. */
  abstract class BaseTypeExpression implements TypeExpression {
    @Override
    public final <R, E extends Throwable> R accept(final TypeVisitor<R, E> typeVisitor) throws E {
      if (typeVisitor instanceof TypeExpressionVisitor) {
        return acceptE((TypeExpressionVisitor<R, E>) typeVisitor);
      }
      throw new RequiredTypeExpressionVisitorException();
    }

    abstract <R, E extends Throwable> R acceptE(
        final TypeExpressionVisitor<R, E> parameterizedTypeVisitor) throws E;
  }

  /** A fixed-length character type expression with a length given by an expression. */
  @Value.Immutable
  abstract class FixedChar extends BaseTypeExpression implements NullableType {
    /**
     * Returns the length expression.
     *
     * @return the length expression
     */
    public abstract TypeExpression length();

    /**
     * Creates a builder for {@link FixedChar}.
     *
     * @return a new builder
     */
    public static ImmutableTypeExpression.FixedChar.Builder builder() {
      return ImmutableTypeExpression.FixedChar.builder();
    }

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  /** A variable-length character type expression with a length given by an expression. */
  @Value.Immutable
  abstract class VarChar extends BaseTypeExpression implements NullableType {
    /**
     * Returns the length expression.
     *
     * @return the length expression
     */
    public abstract TypeExpression length();

    /**
     * Creates a builder for {@link VarChar}.
     *
     * @return a new builder
     */
    public static ImmutableTypeExpression.VarChar.Builder builder() {
      return ImmutableTypeExpression.VarChar.builder();
    }

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  /** A fixed-length binary type expression with a length given by an expression. */
  @Value.Immutable
  abstract class FixedBinary extends BaseTypeExpression implements NullableType {
    /**
     * Returns the length expression.
     *
     * @return the length expression
     */
    public abstract TypeExpression length();

    /**
     * Creates a builder for {@link FixedBinary}.
     *
     * @return a new builder
     */
    public static ImmutableTypeExpression.FixedBinary.Builder builder() {
      return ImmutableTypeExpression.FixedBinary.builder();
    }

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  /** A decimal type expression with precision and scale given by expressions. */
  @Value.Immutable
  abstract class Decimal extends BaseTypeExpression implements NullableType {
    /**
     * Returns the scale expression.
     *
     * @return the scale expression
     */
    public abstract TypeExpression scale();

    /**
     * Returns the precision expression.
     *
     * @return the precision expression
     */
    public abstract TypeExpression precision();

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }

    /**
     * Creates a builder for {@link Decimal}.
     *
     * @return a new builder
     */
    public static ImmutableTypeExpression.Decimal.Builder builder() {
      return ImmutableTypeExpression.Decimal.builder();
    }
  }

  /** A day-time interval type expression with a precision given by an expression. */
  @Value.Immutable
  abstract class IntervalDay extends BaseTypeExpression implements NullableType {

    /**
     * Returns the precision expression.
     *
     * @return the precision expression
     */
    public abstract TypeExpression precision();

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }

    /**
     * Creates a builder for {@link IntervalDay}.
     *
     * @return a new builder
     */
    public static ImmutableTypeExpression.IntervalDay.Builder builder() {
      return ImmutableTypeExpression.IntervalDay.builder();
    }
  }

  /** A compound interval type expression with a precision given by an expression. */
  @Value.Immutable
  abstract class IntervalCompound extends BaseTypeExpression implements NullableType {

    /**
     * Returns the precision expression.
     *
     * @return the precision expression
     */
    public abstract TypeExpression precision();

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }

    /**
     * Creates a builder for {@link IntervalCompound}.
     *
     * @return a new builder
     */
    public static ImmutableTypeExpression.IntervalCompound.Builder builder() {
      return ImmutableTypeExpression.IntervalCompound.builder();
    }
  }

  /** A precision-time type expression with a precision given by an expression. */
  @Value.Immutable
  abstract class PrecisionTime extends BaseTypeExpression implements NullableType {

    /**
     * Returns the precision expression.
     *
     * @return the precision expression
     */
    public abstract TypeExpression precision();

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }

    /**
     * Creates a builder for {@link PrecisionTime}.
     *
     * @return a new builder
     */
    public static ImmutableTypeExpression.PrecisionTime.Builder builder() {
      return ImmutableTypeExpression.PrecisionTime.builder();
    }
  }

  /**
   * A precision-timestamp (without timezone) type expression with a precision given by an
   * expression.
   */
  @Value.Immutable
  abstract class PrecisionTimestamp extends BaseTypeExpression implements NullableType {

    /**
     * Returns the precision expression.
     *
     * @return the precision expression
     */
    public abstract TypeExpression precision();

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }

    /**
     * Creates a builder for {@link PrecisionTimestamp}.
     *
     * @return a new builder
     */
    public static ImmutableTypeExpression.PrecisionTimestamp.Builder builder() {
      return ImmutableTypeExpression.PrecisionTimestamp.builder();
    }
  }

  /**
   * A precision-timestamp with timezone type expression with a precision given by an expression.
   */
  @Value.Immutable
  abstract class PrecisionTimestampTZ extends BaseTypeExpression implements NullableType {

    /**
     * Returns the precision expression.
     *
     * @return the precision expression
     */
    public abstract TypeExpression precision();

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }

    /**
     * Creates a builder for {@link PrecisionTimestampTZ}.
     *
     * @return a new builder
     */
    public static ImmutableTypeExpression.PrecisionTimestampTZ.Builder builder() {
      return ImmutableTypeExpression.PrecisionTimestampTZ.builder();
    }
  }

  /** A struct type expression whose fields are themselves type expressions. */
  @Value.Immutable
  abstract class Struct extends BaseTypeExpression implements NullableType {
    /**
     * Returns the field type expressions.
     *
     * @return the field expressions
     */
    public abstract java.util.List<TypeExpression> fields();

    /**
     * Creates a builder for {@link Struct}.
     *
     * @return a new builder
     */
    public static ImmutableTypeExpression.Struct.Builder builder() {
      return ImmutableTypeExpression.Struct.builder();
    }

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  /** A list type expression whose element type is a type expression. */
  @Value.Immutable
  abstract class ListType extends BaseTypeExpression implements NullableType {
    /**
     * Returns the element type expression.
     *
     * @return the element expression
     */
    public abstract TypeExpression elementType();

    /**
     * Creates a builder for {@link ListType}.
     *
     * @return a new builder
     */
    public static ImmutableTypeExpression.ListType.Builder builder() {
      return ImmutableTypeExpression.ListType.builder();
    }

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  /** A map type expression whose key and value types are type expressions. */
  @Value.Immutable
  abstract class Map extends BaseTypeExpression implements NullableType {
    /**
     * Returns the key type expression.
     *
     * @return the key expression
     */
    public abstract TypeExpression key();

    /**
     * Returns the value type expression.
     *
     * @return the value expression
     */
    public abstract TypeExpression value();

    /**
     * Creates a builder for {@link Map}.
     *
     * @return a new builder
     */
    public static ImmutableTypeExpression.Map.Builder builder() {
      return ImmutableTypeExpression.Map.builder();
    }

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  /** A function type expression whose parameter and return types are type expressions. */
  @Value.Immutable
  abstract class Func extends BaseTypeExpression implements NullableType {
    /**
     * Returns the parameter type expressions.
     *
     * @return the parameter expressions
     */
    public abstract java.util.List<TypeExpression> parameterTypes();

    /**
     * Returns the return type expression.
     *
     * @return the return expression
     */
    public abstract TypeExpression returnType();

    /**
     * Creates a builder for {@link Func}.
     *
     * @return a new builder
     */
    public static ImmutableTypeExpression.Func.Builder builder() {
      return ImmutableTypeExpression.Func.builder();
    }

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  /** A binary operation applied to two type expressions. */
  @Value.Immutable
  abstract class BinaryOperation extends BaseTypeExpression {
    /** The operators supported by a {@link BinaryOperation}. */
    public enum OpType {
      /** Addition. */
      ADD,
      /** Subtraction. */
      SUBTRACT,
      /** Multiplication. */
      MULTIPLY,
      /** Division. */
      DIVIDE,
      /** Minimum of the two operands. */
      MIN,
      /** Maximum of the two operands. */
      MAX,
      /** Less-than comparison. */
      LT,
      /** Greater-than comparison. */
      GT,
      /** Less-than-or-equal comparison. */
      LTE,
      /** Greater-than-or-equal comparison. */
      GTE,
      /** Logical AND. */
      AND,
      /** Logical OR. */
      OR,
      /** Equality comparison. */
      EQ,
      /** Inequality comparison. */
      NOT_EQ,
      /** Whether the left operand covers (contains) the right operand. */
      COVERS
    }

    /**
     * Returns the operator applied by this operation.
     *
     * @return the operator
     */
    public abstract OpType opType();

    /**
     * Returns the left operand.
     *
     * @return the left operand expression
     */
    public abstract TypeExpression left();

    /**
     * Returns the right operand.
     *
     * @return the right operand expression
     */
    public abstract TypeExpression right();

    /**
     * Creates a builder for {@link BinaryOperation}.
     *
     * @return a new builder
     */
    public static ImmutableTypeExpression.BinaryOperation.Builder builder() {
      return ImmutableTypeExpression.BinaryOperation.builder();
    }

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  /** A logical-not operation applied to a type expression. */
  @Value.Immutable
  abstract class NotOperation extends BaseTypeExpression {
    /**
     * Returns the operand expression being negated.
     *
     * @return the inner expression
     */
    public abstract TypeExpression inner();

    /**
     * Creates a builder for {@link NotOperation}.
     *
     * @return a new builder
     */
    public static ImmutableTypeExpression.NotOperation.Builder builder() {
      return ImmutableTypeExpression.NotOperation.builder();
    }

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  /** A conditional (if/then/else) type expression. */
  @Value.Immutable
  abstract class IfOperation extends BaseTypeExpression {
    /**
     * Returns the condition expression.
     *
     * @return the condition expression
     */
    public abstract TypeExpression ifCondition();

    /**
     * Returns the expression evaluated when the condition holds.
     *
     * @return the then expression
     */
    public abstract TypeExpression thenExpr();

    /**
     * Returns the expression evaluated when the condition does not hold.
     *
     * @return the else expression
     */
    public abstract TypeExpression elseExpr();

    /**
     * Creates a builder for {@link IfOperation}.
     *
     * @return a new builder
     */
    public static ImmutableTypeExpression.IfOperation.Builder builder() {
      return ImmutableTypeExpression.IfOperation.builder();
    }

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  /** An integer-literal type expression. */
  @Value.Immutable
  abstract class IntegerLiteral extends BaseTypeExpression {
    /**
     * Returns the integer value.
     *
     * @return the value
     */
    public abstract int value();

    /**
     * Creates a builder for {@link IntegerLiteral}.
     *
     * @return a new builder
     */
    public static ImmutableTypeExpression.IntegerLiteral.Builder builder() {
      return ImmutableTypeExpression.IntegerLiteral.builder();
    }

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  /** A type derivation program: a sequence of named assignments followed by a final expression. */
  @Value.Immutable
  abstract class ReturnProgram extends BaseTypeExpression {
    /**
     * Returns the assignments evaluated before the final expression.
     *
     * @return the assignments
     */
    public abstract java.util.List<Assignment> assignments();

    /**
     * Returns the final expression the program evaluates to.
     *
     * @return the final expression
     */
    public abstract TypeExpression finalExpression();

    /** A single named assignment within a {@link ReturnProgram}. */
    @Value.Immutable
    public abstract static class Assignment {
      /**
       * Returns the name bound by this assignment.
       *
       * @return the assignment name
       */
      public abstract java.lang.String name();

      /**
       * Returns the expression assigned to the name.
       *
       * @return the assigned expression
       */
      public abstract TypeExpression expr();

      /**
       * Creates a builder for {@link Assignment}.
       *
       * @return a new builder
       */
      public static ImmutableTypeExpression.Assignment.Builder builder() {
        return ImmutableTypeExpression.Assignment.builder();
      }
    }

    /**
     * Creates a builder for {@link ReturnProgram}.
     *
     * @return a new builder
     */
    public static ImmutableTypeExpression.ReturnProgram.Builder builder() {
      return ImmutableTypeExpression.ReturnProgram.builder();
    }

    @Override
    <R, E extends Throwable> R acceptE(final TypeExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }
}
