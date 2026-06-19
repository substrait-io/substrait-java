package io.substrait.relation.physical;

import io.substrait.relation.RelVisitor;
import io.substrait.util.VisitationContext;
import org.immutables.value.Value;

/** Exchange relation that distributes rows across destinations in a round-robin fashion. */
@Value.Immutable
public abstract class RoundRobinExchange extends AbstractExchangeRel {
  /**
   * Returns whether the distribution must be exact (rows spread as evenly as possible) rather than
   * approximate.
   *
   * @return {@code true} if an exact round-robin distribution is required
   */
  public abstract boolean getExact();

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a builder for {@link RoundRobinExchange}.
   *
   * @return a new builder
   */
  public static ImmutableRoundRobinExchange.Builder builder() {
    return ImmutableRoundRobinExchange.builder();
  }
}
