package io.substrait.relation;

import org.immutables.value.Value;

@Value.Immutable
public abstract class ExtensionTable extends AbstractReadRel {

  public abstract Extension.ExtensionTableDetail getDetail();

  @Override
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
  }

  public static ImmutableExtensionTable.Builder from(Extension.ExtensionTableDetail detail) {
    return ImmutableExtensionTable.builder().initialSchema(detail.deriveSchema()).detail(detail);
  }
}
