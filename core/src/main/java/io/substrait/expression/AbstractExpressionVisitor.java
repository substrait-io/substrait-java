package io.substrait.expression;

import io.substrait.util.VisitationContext;

public abstract class AbstractExpressionVisitor<O, C extends VisitationContext, E extends Exception>
    implements ExpressionVisitor<O, C, E> {
  public abstract O visitFallback(Expression expr, C context);

  @Override
  public O visit(final Expression.NullLiteral expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.BoolLiteral expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.I8Literal expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.I16Literal expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.I32Literal expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.I64Literal expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.FP32Literal expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.FP64Literal expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.StrLiteral expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.BinaryLiteral expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.TimeLiteral expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.DateLiteral expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.TimestampLiteral expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.TimestampTZLiteral expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.PrecisionTimestampLiteral expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.PrecisionTimestampTZLiteral expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.IntervalYearLiteral expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.IntervalDayLiteral expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.IntervalCompoundLiteral expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.UUIDLiteral expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.FixedCharLiteral expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.VarCharLiteral expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.FixedBinaryLiteral expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.DecimalLiteral expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.MapLiteral expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.EmptyMapLiteral expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.ListLiteral expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.EmptyListLiteral expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.StructLiteral expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.Switch expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.IfThen expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.ScalarFunctionInvocation expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.WindowFunctionInvocation expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.Cast expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.SingleOrList expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.MultiOrList expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final FieldReference expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.SetPredicate expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.ScalarSubquery expr, final C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(final Expression.InPredicate expr, final C context) throws E {
    return visitFallback(expr, context);
  }
}
