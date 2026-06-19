package io.substrait.relation.physical;

import io.substrait.expression.Expression;
import io.substrait.relation.RelVisitor;
import io.substrait.util.VisitationContext;
import org.immutables.value.Value;

/**
 * Exchange relation that assigns each row to a single bucket computed from an expression, routing
 * the row to the destination identified by that bucket.
 */
@Value.Immutable
public abstract class SingleBucketExchange extends AbstractExchangeRel {
  /**
   * Returns the expression used to compute the destination bucket for each row.
   *
   * @return the bucket expression
   */
  public abstract Expression getExpression();

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a builder for {@link SingleBucketExchange}.
   *
   * @return a new builder
   */
  public static ImmutableSingleBucketExchange.Builder builder() {
    return ImmutableSingleBucketExchange.builder();
  }
}
