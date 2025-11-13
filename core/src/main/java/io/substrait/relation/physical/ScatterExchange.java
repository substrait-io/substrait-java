package io.substrait.relation.physical;

import io.substrait.expression.FieldReference;
import io.substrait.relation.RelVisitor;
import io.substrait.util.VisitationContext;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
public abstract class ScatterExchange extends AbstractExchangeRel {
  public abstract List<FieldReference> getFields();

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  public static ImmutableScatterExchange.Builder builder() {
    return ImmutableScatterExchange.builder();
  }
}
