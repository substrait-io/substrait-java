package io.substrait.expression;

import io.substrait.util.VisitationContext;

public interface ExpressionVisitor<R, C extends VisitationContext, E extends Throwable> {

  R visit(Expression.NullLiteral expr, C context) throws E;

  R visit(Expression.BoolLiteral expr, C context) throws E;

  R visit(Expression.I8Literal expr, C context) throws E;

  R visit(Expression.I16Literal expr, C context) throws E;

  R visit(Expression.I32Literal expr, C context) throws E;

  R visit(Expression.I64Literal expr, C context) throws E;

  R visit(Expression.FP32Literal expr, C context) throws E;

  R visit(Expression.FP64Literal expr, C context) throws E;

  R visit(Expression.StrLiteral expr, C context) throws E;

  R visit(Expression.BinaryLiteral expr, C context) throws E;

  R visit(Expression.TimeLiteral expr, C context) throws E;

  R visit(Expression.DateLiteral expr, C context) throws E;

  R visit(Expression.TimestampLiteral expr, C context) throws E;

  R visit(Expression.TimestampTZLiteral expr, C context) throws E;

  R visit(Expression.PrecisionTimestampLiteral expr, C context) throws E;

  R visit(Expression.PrecisionTimestampTZLiteral expr, C context) throws E;

  R visit(Expression.IntervalYearLiteral expr, C context) throws E;

  R visit(Expression.IntervalDayLiteral expr, C context) throws E;

  R visit(Expression.IntervalCompoundLiteral expr, C context) throws E;

  R visit(Expression.UUIDLiteral expr, C context) throws E;

  R visit(Expression.FixedCharLiteral expr, C context) throws E;

  R visit(Expression.VarCharLiteral expr, C context) throws E;

  R visit(Expression.FixedBinaryLiteral expr, C context) throws E;

  R visit(Expression.DecimalLiteral expr, C context) throws E;

  R visit(Expression.MapLiteral expr, C context) throws E;

  R visit(Expression.EmptyMapLiteral expr, C context) throws E;

  R visit(Expression.ListLiteral expr, C context) throws E;

  R visit(Expression.EmptyListLiteral expr, C context) throws E;

  R visit(Expression.StructLiteral expr, C context) throws E;

  R visit(Expression.NestedStruct expr, C context) throws E;

  R visit(Expression.UserDefinedLiteral expr, C context) throws E;

  R visit(Expression.Switch expr, C context) throws E;

  R visit(Expression.IfThen expr, C context) throws E;

  R visit(Expression.ScalarFunctionInvocation expr, C context) throws E;

  R visit(Expression.WindowFunctionInvocation expr, C context) throws E;

  R visit(Expression.Cast expr, C context) throws E;

  R visit(Expression.SingleOrList expr, C context) throws E;

  R visit(Expression.MultiOrList expr, C context) throws E;

  R visit(Expression.NestedList expr, C context) throws E;

  R visit(FieldReference expr, C context) throws E;

  R visit(Expression.SetPredicate expr, C context) throws E;

  R visit(Expression.ScalarSubquery expr, C context) throws E;

  R visit(Expression.InPredicate expr, C context) throws E;
}
