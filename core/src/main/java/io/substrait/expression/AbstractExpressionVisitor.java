package io.substrait.expression;

import io.substrait.util.VisitationContext;

/**
 * Base expression visitor that routes all visits to a fallback handler. Subclasses can override
 * specific visit methods as needed.
 *
 * @param <O> the visit result type
 * @param <C> the visitation context type
 * @param <E> the checked exception type thrown during visitation
 */
public abstract class AbstractExpressionVisitor<O, C extends VisitationContext, E extends Exception>
    implements ExpressionVisitor<O, C, E> {

  /**
   * Fallback handler for expressions not explicitly overridden.
   *
   * @param expr the expression to visit
   * @param context the visitation context
   * @return the visit result
   */
  public abstract O visitFallback(Expression expr, C context);

  /**
   * Visits a null literal.
   *
   * @param expr the null literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.NullLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a boolean literal.
   *
   * @param expr the boolean literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.BoolLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits an 8-bit integer literal.
   *
   * @param expr the I8 literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.I8Literal expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a 16-bit integer literal.
   *
   * @param expr the I16 literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.I16Literal expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a 32-bit integer literal.
   *
   * @param expr the I32 literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.I32Literal expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a 64-bit integer literal.
   *
   * @param expr the I64 literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.I64Literal expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a 32-bit floating-point literal.
   *
   * @param expr the FP32 literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.FP32Literal expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a 64-bit floating-point literal.
   *
   * @param expr the FP64 literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.FP64Literal expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a string literal.
   *
   * @param expr the string literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.StrLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a binary literal.
   *
   * @param expr the binary literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.BinaryLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a time literal.
   *
   * @param expr the time literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.TimeLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a date literal.
   *
   * @param expr the date literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.DateLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a timestamp literal.
   *
   * @param expr the timestamp literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.TimestampLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a timestamp-with-time-zone literal.
   *
   * @param expr the timestamp TZ literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.TimestampTZLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a precision timestamp literal.
   *
   * @param expr the precision timestamp literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.PrecisionTimestampLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a precision timestamp-with-time-zone literal.
   *
   * @param expr the precision timestamp TZ literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.PrecisionTimestampTZLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits an interval (years/months) literal.
   *
   * @param expr the year interval literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.IntervalYearLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits an interval (days/time) literal.
   *
   * @param expr the day interval literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.IntervalDayLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a compound interval literal.
   *
   * @param expr the compound interval literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.IntervalCompoundLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a UUID literal.
   *
   * @param expr the UUID literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.UUIDLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a fixed-length character literal.
   *
   * @param expr the fixed char literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.FixedCharLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a variable-length character literal.
   *
   * @param expr the varchar literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.VarCharLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a fixed-length binary literal.
   *
   * @param expr the fixed binary literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.FixedBinaryLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a decimal literal.
   *
   * @param expr the decimal literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.DecimalLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a map literal.
   *
   * @param expr the map literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.MapLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits an empty map literal.
   *
   * @param expr the empty map literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.EmptyMapLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a list literal.
   *
   * @param expr the list literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.ListLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits an empty list literal.
   *
   * @param expr the empty list literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.EmptyListLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a struct literal.
   *
   * @param expr the struct literal
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.StructLiteral expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a nested struct expression.
   *
   * @param expr the nested struct
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
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

  /**
   * Visits a switch expression.
   *
   * @param expr the switch expression
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.Switch expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits an if-then expression.
   *
   * @param expr the if-then expression
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.IfThen expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a scalar function invocation.
   *
   * @param expr the scalar function invocation
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.ScalarFunctionInvocation expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a window function invocation.
   *
   * @param expr the window function invocation
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.WindowFunctionInvocation expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a cast expression.
   *
   * @param expr the cast expression
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.Cast expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a single-or-list expression.
   *
   * @param expr the single-or-list
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.SingleOrList expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a multi-or-list expression.
   *
   * @param expr the multi-or-list
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.MultiOrList expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a nested list expression.
   *
   * @param expr the nested list
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.NestedList expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a field reference.
   *
   * @param expr the field reference
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(FieldReference expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a set predicate.
   *
   * @param expr the set predicate
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.SetPredicate expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits a scalar subquery.
   *
   * @param expr the scalar subquery
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.ScalarSubquery expr, C context) throws E {
    return visitFallback(expr, context);
  }

  /**
   * Visits an IN predicate.
   *
   * @param expr the IN predicate
   * @param context the visitation context
   * @return the visit result
   * @throws E if visitation fails
   */
  @Override
  public O visit(Expression.InPredicate expr, C context) throws E {
    return visitFallback(expr, context);
  }
}
