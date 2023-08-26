package io.substrait.expression;

public abstract class AbstractExpressionVisitor<OUTPUT, EXCEPTION extends Exception>
    implements ExpressionVisitor<OUTPUT, EXCEPTION> {
  public abstract OUTPUT visitFallback(Expression expr);

  @Override
  public OUTPUT visit(Expression.NullLiteral expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.BoolLiteral expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.I8Literal expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.I16Literal expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.I32Literal expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.I64Literal expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.FP32Literal expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.FP64Literal expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.StrLiteral expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.BinaryLiteral expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.TimeLiteral expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.DateLiteral expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.TimestampLiteral expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.TimestampTZLiteral expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.IntervalYearLiteral expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.IntervalDayLiteral expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.UUIDLiteral expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.FixedCharLiteral expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.VarCharLiteral expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.FixedBinaryLiteral expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.DecimalLiteral expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.MapLiteral expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.ListLiteral expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.StructLiteral expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.Switch expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.IfThen expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.ScalarFunctionInvocation expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(WindowFunctionInvocation expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.Cast expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.SingleOrList expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.MultiOrList expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(FieldReference expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.SetPredicate expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.ScalarSubquery expr) throws EXCEPTION {
    return visitFallback(expr);
  }

  @Override
  public OUTPUT visit(Expression.InPredicate expr) throws EXCEPTION {
    return visitFallback(expr);
  }
}
