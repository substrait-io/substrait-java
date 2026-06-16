package io.substrait.function;

/**
 * Visitor over the concrete {@link TypeExpression} kinds, extending {@link
 * ParameterizedTypeVisitor} with the derivation-expression variants.
 *
 * @param <R> the result type produced by the visitor
 * @param <E> the exception type that may be thrown
 */
public interface TypeExpressionVisitor<R, E extends Throwable>
    extends ParameterizedTypeVisitor<R, E> {
  /**
   * Visits a fixed-length character type expression.
   *
   * @param expr the expression being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(TypeExpression.FixedChar expr) throws E;

  /**
   * Visits a variable-length character type expression.
   *
   * @param expr the expression being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(TypeExpression.VarChar expr) throws E;

  /**
   * Visits a fixed-length binary type expression.
   *
   * @param expr the expression being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(TypeExpression.FixedBinary expr) throws E;

  /**
   * Visits a decimal type expression.
   *
   * @param expr the expression being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(TypeExpression.Decimal expr) throws E;

  /**
   * Visits a day-time interval type expression.
   *
   * @param expr the expression being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(TypeExpression.IntervalDay expr) throws E;

  /**
   * Visits a compound interval type expression.
   *
   * @param expr the expression being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(TypeExpression.IntervalCompound expr) throws E;

  /**
   * Visits a precision-time type expression.
   *
   * @param expr the expression being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(TypeExpression.PrecisionTime expr) throws E;

  /**
   * Visits a precision-timestamp (without timezone) type expression.
   *
   * @param expr the expression being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(TypeExpression.PrecisionTimestamp expr) throws E;

  /**
   * Visits a precision-timestamp with timezone type expression.
   *
   * @param expr the expression being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(TypeExpression.PrecisionTimestampTZ expr) throws E;

  /**
   * Visits a struct type expression.
   *
   * @param expr the expression being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(TypeExpression.Struct expr) throws E;

  /**
   * Visits a list type expression.
   *
   * @param expr the expression being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(TypeExpression.ListType expr) throws E;

  /**
   * Visits a map type expression.
   *
   * @param expr the expression being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(TypeExpression.Map expr) throws E;

  /**
   * Visits a function type expression.
   *
   * @param expr the expression being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(TypeExpression.Func expr) throws E;

  /**
   * Visits a binary operation type expression.
   *
   * @param expr the expression being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(TypeExpression.BinaryOperation expr) throws E;

  /**
   * Visits a logical-not operation type expression.
   *
   * @param expr the expression being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(TypeExpression.NotOperation expr) throws E;

  /**
   * Visits an if/then operation type expression.
   *
   * @param expr the expression being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(TypeExpression.IfOperation expr) throws E;

  /**
   * Visits an integer-literal type expression.
   *
   * @param expr the expression being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(TypeExpression.IntegerLiteral expr) throws E;

  /**
   * Visits a return-program type expression.
   *
   * @param expr the expression being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(TypeExpression.ReturnProgram expr) throws E;

  /**
   * Base {@link TypeExpressionVisitor} that throws {@link UnsupportedOperationException} for every
   * expression, allowing subclasses to override only the visit methods they support.
   *
   * @param <R> the result type produced by the visitor
   * @param <E> the exception type that may be thrown
   */
  abstract class TypeExpressionThrowsVisitor<R, E extends Throwable>
      extends ParameterizedTypeVisitor.ParameterizedTypeThrowsVisitor<R, E>
      implements TypeExpressionVisitor<R, E> {

    /**
     * Creates a visitor that throws with the given message for unsupported expressions.
     *
     * @param unsupportedMessage the message used for unsupported expressions
     */
    protected TypeExpressionThrowsVisitor(String unsupportedMessage) {
      super(unsupportedMessage);
    }

    @Override
    public R visit(TypeExpression.FixedChar expr) throws E {
      throw t();
    }

    @Override
    public R visit(TypeExpression.VarChar expr) throws E {
      throw t();
    }

    @Override
    public R visit(TypeExpression.FixedBinary expr) throws E {
      throw t();
    }

    @Override
    public R visit(TypeExpression.Decimal expr) throws E {
      throw t();
    }

    @Override
    public R visit(TypeExpression.PrecisionTime expr) throws E {
      throw t();
    }

    @Override
    public R visit(TypeExpression.PrecisionTimestamp expr) throws E {
      throw t();
    }

    @Override
    public R visit(TypeExpression.PrecisionTimestampTZ expr) throws E {
      throw t();
    }

    @Override
    public R visit(TypeExpression.IntervalDay expr) throws E {
      throw t();
    }

    @Override
    public R visit(TypeExpression.IntervalCompound expr) throws E {
      throw t();
    }

    @Override
    public R visit(TypeExpression.Struct expr) throws E {
      throw t();
    }

    @Override
    public R visit(TypeExpression.ListType expr) throws E {
      throw t();
    }

    @Override
    public R visit(TypeExpression.Map expr) throws E {
      throw t();
    }

    @Override
    public R visit(TypeExpression.Func expr) throws E {
      throw t();
    }

    @Override
    public R visit(TypeExpression.BinaryOperation expr) throws E {
      throw t();
    }

    @Override
    public R visit(TypeExpression.NotOperation expr) throws E {
      throw t();
    }

    @Override
    public R visit(TypeExpression.IfOperation expr) throws E {
      throw t();
    }

    @Override
    public R visit(TypeExpression.IntegerLiteral expr) throws E {
      throw t();
    }

    @Override
    public R visit(TypeExpression.ReturnProgram expr) throws E {
      throw t();
    }
  }
}
