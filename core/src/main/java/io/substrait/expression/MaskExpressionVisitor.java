package io.substrait.expression;

import io.substrait.util.VisitationContext;

/**
 * Visitor for {@link MaskExpression} select nodes.
 *
 * @param <R> result type returned by each visit
 * @param <C> visitation context type
 * @param <E> throwable type that visit methods may throw
 */
public interface MaskExpressionVisitor<R, C extends VisitationContext, E extends Throwable> {

  /**
   * Visit a struct select.
   *
   * @param structSelect the struct select
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(MaskExpression.StructSelect structSelect, C context) throws E;

  /**
   * Visit a list select.
   *
   * @param listSelect the list select
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(MaskExpression.ListSelect listSelect, C context) throws E;

  /**
   * Visit a map select.
   *
   * @param mapSelect the map select
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  R visit(MaskExpression.MapSelect mapSelect, C context) throws E;
}
