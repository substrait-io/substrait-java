package io.substrait.expression;

import io.substrait.util.VisitationContext;

public abstract class AbstractExpressionVisitor<O, C extends VisitationContext, E extends Exception>
    implements ExpressionVisitor<O, C, E> {
  public abstract O visitFallback(Expression expr, C context);

  @Override
  public O visit(Expression.NullLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.BoolLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.I8Literal expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.I16Literal expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.I32Literal expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.I64Literal expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.FP32Literal expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.FP64Literal expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.StrLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.BinaryLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.TimeLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.DateLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.TimestampLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.TimestampTZLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.PrecisionTimestampLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.PrecisionTimestampTZLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.IntervalYearLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.IntervalDayLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.IntervalCompoundLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.UUIDLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.FixedCharLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.VarCharLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.FixedBinaryLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.DecimalLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.MapLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.EmptyMapLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.ListLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.EmptyListLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.StructLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.UserDefinedAnyLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.UserDefinedStructLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.NestedStruct expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.Switch expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.IfThen expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.ScalarFunctionInvocation expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.WindowFunctionInvocation expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.Cast expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.SingleOrList expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.MultiOrList expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.NestedList expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(FieldReference expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.SetPredicate expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.ScalarSubquery expr, C context) throws E {
    return visitFallback(expr, context);
  }

  @Override
  public O visit(Expression.InPredicate expr, C context) throws E {
    return visitFallback(expr, context);
  }
}
