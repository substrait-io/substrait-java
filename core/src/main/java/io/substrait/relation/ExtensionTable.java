package io.substrait.relation;

import io.substrait.util.VisitationContext;
import org.immutables.value.Value;

@Value.Immutable
public abstract class ExtensionTable extends AbstractReadRel {

  public abstract Extension.ExtensionTableDetail getDetail();

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  public static ImmutableExtensionTable.Builder from(Extension.ExtensionTableDetail detail) {
    return ImmutableExtensionTable.builder().initialSchema(detail.deriveSchema()).detail(detail);
  }

  public static ImmutableExtensionTable.Builder builder() {
    return ImmutableExtensionTable.builder();
  }
}
