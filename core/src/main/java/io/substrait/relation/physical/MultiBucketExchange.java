package io.substrait.relation.physical;

import io.substrait.expression.Expression;
import io.substrait.relation.RelVisitor;
import io.substrait.util.VisitationContext;
import org.immutables.value.Value;

/**
 * Exchange relation that assigns each row to one of several buckets computed from an expression,
 * distributing rows across destinations accordingly.
 */
@Value.Immutable
public abstract class MultiBucketExchange extends AbstractExchangeRel {
  /**
   * Returns the expression used to compute the destination bucket for each row.
   *
   * @return the bucket expression
   */
  public abstract Expression getExpression();

  /**
   * Returns whether the number of buckets is constrained to the number of partitions/destinations.
   *
   * @return {@code true} if the bucket count is constrained to the partition count
   */
  public abstract boolean getConstrainedToCount();

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a builder for {@link MultiBucketExchange}.
   *
   * @return a new builder
   */
  public static ImmutableMultiBucketExchange.Builder builder() {
    return ImmutableMultiBucketExchange.builder();
  }
}
