package io.substrait.relation;

import io.substrait.util.VisitationContext;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Immutable
public abstract class ExtensionMulti extends AbstractRel {

  public abstract Extension.MultiRelDetail getDetail();

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      final RelVisitor<O, C, E> visitor, final C context) throws E {
    return visitor.visit(this, context);
  }

  public static ImmutableExtensionMulti.Builder from(
      final Extension.MultiRelDetail detail, final Rel... inputs) {
    return from(detail, Arrays.stream(inputs).collect(Collectors.toList()));
  }

  public static ImmutableExtensionMulti.Builder from(
      final Extension.MultiRelDetail detail, final List<Rel> inputs) {
    return ImmutableExtensionMulti.builder()
        .addAllInputs(inputs)
        .detail(detail)
        .deriveRecordType(detail.deriveRecordType(inputs));
  }

  public static ImmutableExtensionMulti.Builder builder() {
    return ImmutableExtensionMulti.builder();
  }
}
