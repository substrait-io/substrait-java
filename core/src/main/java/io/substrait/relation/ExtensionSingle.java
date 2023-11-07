package io.substrait.relation;

import org.immutables.value.Value;

@Value.Immutable
public abstract class ExtensionSingle extends SingleInputRel {

  public abstract Extension.SingleRelDetail getDetail();

  @Override
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
  }

  public static ImmutableExtensionSingle.Builder from(Extension.SingleRelDetail detail, Rel input) {
    return ImmutableExtensionSingle.builder()
        .input(input)
        .detail(detail)
        .deriveRecordType(detail.deriveRecordType(input));
  }

  public static ImmutableExtensionSingle.Builder builder() {
    return ImmutableExtensionSingle.builder();
  }
}
