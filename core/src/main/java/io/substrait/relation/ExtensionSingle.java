package io.substrait.relation;

import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public abstract class ExtensionSingle extends SingleInputRel {

  public abstract Optional<Extension.SingleRelDetail> getDetail();

  @Override
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
  }

  public static ImmutableExtensionSingle.Builder builder() {
    return ImmutableExtensionSingle.builder();
  }
}
