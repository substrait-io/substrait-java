package io.substrait.relation;

import org.immutables.value.Value;

@Value.Immutable
public abstract class ExtensionMulti extends AbstractRel {

  public abstract Extension.MultiRelDetail getDetail();

  @Override
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
  }

  public static ImmutableExtensionMulti.Builder from(
      Extension.MultiRelDetail detail, Rel... inputs) {
    return ImmutableExtensionMulti.builder()
        .addInputs(inputs)
        .detail(detail)
        .deriveRecordType(detail.deriveRecordType(inputs));
  }

  public static ImmutableExtensionMulti.Builder builder() {
    return ImmutableExtensionMulti.builder();
  }
}
