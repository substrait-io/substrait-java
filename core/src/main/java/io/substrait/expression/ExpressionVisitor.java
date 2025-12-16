package io.substrait.expression;

import io.substrait.util.VisitationContext;

/**
 * Visitor for {@code Expression} nodes.
 *
 * @param <R> result type returned by each visit
 * @param <C> visitation context type
 * @param <E> throwable type that visit methods may throw
 */
public interface ExpressionVisitor<R, C extends VisitationContext, E extends Throwable> {

  /**
   * Visit a NULL literal.
   *
   * @param expr the NULL literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.NullLiteral expr, C context) throws E;

  /**
   * Visit a boolean literal.
   *
   * @param expr the boolean literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.BoolLiteral expr, C context) throws E;

  /**
   * Visit an 8-bit integer literal.
   *
   * @param expr the i8 literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.I8Literal expr, C context) throws E;

  /**
   * Visit a 16-bit integer literal.
   *
   * @param expr the i16 literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.I16Literal expr, C context) throws E;

  /**
   * Visit a 32-bit integer literal.
   *
   * @param expr the i32 literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.I32Literal expr, C context) throws E;

  /**
   * Visit a 64-bit integer literal.
   *
   * @param expr the i64 literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.I64Literal expr, C context) throws E;

  /**
   * Visit a 32-bit floating-point literal.
   *
   * @param expr the fp32 literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.FP32Literal expr, C context) throws E;

  /**
   * Visit a 64-bit floating-point literal.
   *
   * @param expr the fp64 literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.FP64Literal expr, C context) throws E;

  /**
   * Visit a string literal.
   *
   * @param expr the string literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.StrLiteral expr, C context) throws E;

  /**
   * Visit a binary literal.
   *
   * @param expr the binary literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.BinaryLiteral expr, C context) throws E;

  /**
   * Visit a time literal.
   *
   * @param expr the time literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.TimeLiteral expr, C context) throws E;

  /**
   * Visit a date literal.
   *
   * @param expr the date literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.DateLiteral expr, C context) throws E;

  /**
   * Visit a timestamp literal.
   *
   * @param expr the timestamp literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.TimestampLiteral expr, C context) throws E;

  /**
   * Visit a timestamp-with-timezone literal.
   *
   * @param expr the timestamp TZ literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.TimestampTZLiteral expr, C context) throws E;

  /**
   * Visit a precision timestamp literal.
   *
   * @param expr the precision timestamp literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.PrecisionTimestampLiteral expr, C context) throws E;

  /**
   * Visit a precision timestamp-with-timezone literal.
   *
   * @param expr the precision timestamp TZ literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.PrecisionTimestampTZLiteral expr, C context) throws E;

  /**
   * Visit a year/month interval literal.
   *
   * @param expr the interval (year) literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.IntervalYearLiteral expr, C context) throws E;

  /**
   * Visit a day/time interval literal.
   *
   * @param expr the interval (day) literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.IntervalDayLiteral expr, C context) throws E;

  /**
   * Visit a compound interval literal.
   *
   * @param expr the compound interval literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.IntervalCompoundLiteral expr, C context) throws E;

  /**
   * Visit a UUID literal.
   *
   * @param expr the UUID literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.UUIDLiteral expr, C context) throws E;

  /**
   * Visit a fixed-length char literal.
   *
   * @param expr the fixed char literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.FixedCharLiteral expr, C context) throws E;

  /**
   * Visit a variable-length char literal.
   *
   * @param expr the varchar literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.VarCharLiteral expr, C context) throws E;

  /**
   * Visit a fixed-length binary literal.
   *
   * @param expr the fixed binary literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.FixedBinaryLiteral expr, C context) throws E;

  /**
   * Visit a decimal literal.
   *
   * @param expr the decimal literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.DecimalLiteral expr, C context) throws E;

  /**
   * Visit a map literal.
   *
   * @param expr the map literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.MapLiteral expr, C context) throws E;

  /**
   * Visit an empty map literal.
   *
   * @param expr the empty map literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.EmptyMapLiteral expr, C context) throws E;

  /**
   * Visit a list literal.
   *
   * @param expr the list literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.ListLiteral expr, C context) throws E;

  /**
   * Visit an empty list literal.
   *
   * @param expr the empty list literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.EmptyListLiteral expr, C context) throws E;

  /**
   * Visit a struct literal.
   *
   * @param expr the struct literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.StructLiteral expr, C context) throws E;

  /**
   * Visit a nested struct.
   *
   * @param expr the nested struct
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.NestedStruct expr, C context) throws E;

  /**
   * Visit a user-defined literal.
   *
   * @param expr the user-defined literal
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.UserDefinedLiteral expr, C context) throws E;

  /**
   * Visit a switch expression.
   *
   * @param expr the switch expression
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.Switch expr, C context) throws E;

  /**
   * Visit an if-then expression.
   *
   * @param expr the if-then expression
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.IfThen expr, C context) throws E;

  /**
   * Visit a scalar function invocation.
   *
   * @param expr the scalar function invocation
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.ScalarFunctionInvocation expr, C context) throws E;

  /**
   * Visit a window function invocation.
   *
   * @param expr the window function invocation
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.WindowFunctionInvocation expr, C context) throws E;

  /**
   * Visit a cast.
   *
   * @param expr the cast expression
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.Cast expr, C context) throws E;

  /**
   * Visit a single-or-list expression.
   *
   * @param expr the single-or-list expression
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.SingleOrList expr, C context) throws E;

  /**
   * Visit a multi-or-list expression.
   *
   * @param expr the multi-or-list expression
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.MultiOrList expr, C context) throws E;

  /**
   * Visit a nested list.
   *
   * @param expr the nested list
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.NestedList expr, C context) throws E;

  /**
   * Visit a field reference.
   *
   * @param expr the field reference
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(FieldReference expr, C context) throws E;

  /**
   * Visit a set predicate.
   *
   * @param expr the set predicate
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.SetPredicate expr, C context) throws E;

  /**
   * Visit a scalar subquery.
   *
   * @param expr the scalar subquery
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.ScalarSubquery expr, C context) throws E;

  /**
   * Visit an IN predicate.
   *
   * @param expr the IN predicate
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(Expression.InPredicate expr, C context) throws E;
}
