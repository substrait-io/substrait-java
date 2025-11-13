package io.substrait.relation.physical;

import io.substrait.relation.RelVisitor;
import io.substrait.util.VisitationContext;
import org.immutables.value.Value;

@Value.Immutable
public abstract class BroadcastExchange extends AbstractExchangeRel {
  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  public static ImmutableBroadcastExchange.Builder builder() {
    return ImmutableBroadcastExchange.builder();
  }
}
