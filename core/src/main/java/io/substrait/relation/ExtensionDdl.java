package io.substrait.relation;

import io.substrait.util.VisitationContext;
import org.immutables.value.Value;

@Value.Immutable
public abstract class ExtensionDdl extends AbstractDdlRel implements HasExtension {
  public abstract Extension.DdlExtensionObject getDetail();

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  public static ImmutableExtensionDdl.Builder builder() {
    return ImmutableExtensionDdl.builder();
  }
}
