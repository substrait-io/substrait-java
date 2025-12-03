package io.substrait.relation;

import io.substrait.util.VisitationContext;
import org.immutables.value.Value;

@Value.Immutable
public abstract class ExtensionTable extends AbstractReadRel {

  public abstract Extension.ExtensionTableDetail getDetail();

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      final RelVisitor<O, C, E> visitor, final C context) throws E {
    return visitor.visit(this, context);
  }

  public static ImmutableExtensionTable.Builder from(final Extension.ExtensionTableDetail detail) {
    return ImmutableExtensionTable.builder().initialSchema(detail.deriveSchema()).detail(detail);
  }

  public static ImmutableExtensionTable.Builder builder() {
    return ImmutableExtensionTable.builder();
  }
}
