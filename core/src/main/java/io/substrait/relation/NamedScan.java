package io.substrait.relation;

import io.substrait.util.VisitationContext;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
public abstract class NamedScan extends AbstractReadRel {

  public abstract List<String> getNames();

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      final RelVisitor<O, C, E> visitor, final C context) throws E {
    return visitor.visit(this, context);
  }

  public static ImmutableNamedScan.Builder builder() {
    return ImmutableNamedScan.builder();
  }
}
