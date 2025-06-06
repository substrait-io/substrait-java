package io.substrait.relation;

import org.immutables.value.Value;

@Value.Immutable
public abstract class ExtensionWrite extends AbstractWriteRel implements HasExtension {
  public abstract Extension.WriteExtensionObject getDetail();

  @Override
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
  }

  public static ImmutableExtensionWrite.Builder builder() {
    return ImmutableExtensionWrite.builder();
  }
}
