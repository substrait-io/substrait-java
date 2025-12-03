package io.substrait.function;

public interface TypeExpressionVisitor<R, E extends Throwable>
    extends ParameterizedTypeVisitor<R, E> {
  R visit(TypeExpression.FixedChar expr) throws E;

  R visit(TypeExpression.VarChar expr) throws E;

  R visit(TypeExpression.FixedBinary expr) throws E;

  R visit(TypeExpression.Decimal expr) throws E;

  R visit(TypeExpression.IntervalDay expr) throws E;

  R visit(TypeExpression.IntervalCompound expr) throws E;

  R visit(TypeExpression.PrecisionTimestamp expr) throws E;

  R visit(TypeExpression.PrecisionTimestampTZ expr) throws E;

  R visit(TypeExpression.Struct expr) throws E;

  R visit(TypeExpression.ListType expr) throws E;

  R visit(TypeExpression.Map expr) throws E;

  R visit(TypeExpression.BinaryOperation expr) throws E;

  R visit(TypeExpression.NotOperation expr) throws E;

  R visit(TypeExpression.IfOperation expr) throws E;

  R visit(TypeExpression.IntegerLiteral expr) throws E;

  R visit(TypeExpression.ReturnProgram expr) throws E;

  abstract class TypeExpressionThrowsVisitor<R, E extends Throwable>
      extends ParameterizedTypeVisitor.ParameterizedTypeThrowsVisitor<R, E>
      implements TypeExpressionVisitor<R, E> {

    protected TypeExpressionThrowsVisitor(final String unsupportedMessage) {
      super(unsupportedMessage);
    }

    @Override
    public R visit(final TypeExpression.FixedChar expr) throws E {
      throw t();
    }

    @Override
    public R visit(final TypeExpression.VarChar expr) throws E {
      throw t();
    }

    @Override
    public R visit(final TypeExpression.FixedBinary expr) throws E {
      throw t();
    }

    @Override
    public R visit(final TypeExpression.Decimal expr) throws E {
      throw t();
    }

    @Override
    public R visit(final TypeExpression.PrecisionTimestamp expr) throws E {
      throw t();
    }

    @Override
    public R visit(final TypeExpression.PrecisionTimestampTZ expr) throws E {
      throw t();
    }

    @Override
    public R visit(final TypeExpression.IntervalDay expr) throws E {
      throw t();
    }

    @Override
    public R visit(final TypeExpression.IntervalCompound expr) throws E {
      throw t();
    }

    @Override
    public R visit(final TypeExpression.Struct expr) throws E {
      throw t();
    }

    @Override
    public R visit(final TypeExpression.ListType expr) throws E {
      throw t();
    }

    @Override
    public R visit(final TypeExpression.Map expr) throws E {
      throw t();
    }

    @Override
    public R visit(final TypeExpression.BinaryOperation expr) throws E {
      throw t();
    }

    @Override
    public R visit(final TypeExpression.NotOperation expr) throws E {
      throw t();
    }

    @Override
    public R visit(final TypeExpression.IfOperation expr) throws E {
      throw t();
    }

    @Override
    public R visit(final TypeExpression.IntegerLiteral expr) throws E {
      throw t();
    }

    @Override
    public R visit(final TypeExpression.ReturnProgram expr) throws E {
      throw t();
    }
  }
}
