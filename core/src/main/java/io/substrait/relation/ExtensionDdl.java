package io.substrait.relation;

import org.immutables.value.Value;

@Value.Immutable
public abstract class ExtensionDdl extends AbstractDdlRel implements HasExtension {
  public abstract Extension.DdlExtensionObject getDetail();

  @Override
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
  }

  public static ImmutableExtensionDdl.Builder builder() {
    return ImmutableExtensionDdl.builder();
  }
}
