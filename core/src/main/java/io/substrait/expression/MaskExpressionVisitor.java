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

  R visit(MaskExpression.StructSelect structSelect, C context) throws E;

  R visit(MaskExpression.ListSelect listSelect, C context) throws E;

  R visit(MaskExpression.MapSelect mapSelect, C context) throws E;
}
