package io.substrait.relation.physical;

import io.substrait.relation.RelVisitor;
import io.substrait.util.VisitationContext;
import org.immutables.value.Value;

/**
 * Represents a broadcast exchange that sends all data from the input to all target nodes. This is
 * typically used as a performance optimization to join a small dataset with a large dataset in a
 * distributed data processing engine. A full copy of the small dataset is sent to all processing
 * nodes to reduce network overheads.
 */
@Value.Immutable
public abstract class BroadcastExchange extends AbstractExchangeRel {
  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a new builder for constructing a BroadcastExchange relation.
   *
   * @return a new builder instance
   */
  public static ImmutableBroadcastExchange.Builder builder() {
    return ImmutableBroadcastExchange.builder();
  }
}
