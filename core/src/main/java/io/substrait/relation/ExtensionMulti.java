package io.substrait.relation;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
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
    return from(detail, Arrays.stream(inputs).collect(Collectors.toList()));
  }

  public static ImmutableExtensionMulti.Builder from(
      Extension.MultiRelDetail detail, List<Rel> inputs) {
    return ImmutableExtensionMulti.builder()
        .addAllInputs(inputs)
        .detail(detail)
        .deriveRecordType(detail.deriveRecordType(inputs));
  }
}
