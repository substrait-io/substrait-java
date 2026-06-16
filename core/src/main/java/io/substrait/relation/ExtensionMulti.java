package io.substrait.relation;

import io.substrait.util.VisitationContext;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.immutables.value.Value;

/** A multi-input relation whose behavior is supplied by an extension-defined detail object. */
@Value.Immutable
public abstract class ExtensionMulti extends AbstractRel {

  /**
   * Returns the extension-specific detail describing this relation.
   *
   * @return the extension detail object
   */
  public abstract Extension.MultiRelDetail getDetail();

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a builder initialized from the given extension detail and input relations.
   *
   * @param detail the extension detail describing the relation
   * @param inputs the input relations
   * @return a builder populated from {@code detail} and {@code inputs}
   */
  public static ImmutableExtensionMulti.Builder from(
      Extension.MultiRelDetail detail, Rel... inputs) {
    return from(detail, Arrays.stream(inputs).collect(Collectors.toList()));
  }

  /**
   * Creates a builder initialized from the given extension detail and input relations.
   *
   * @param detail the extension detail describing the relation
   * @param inputs the input relations
   * @return a builder populated from {@code detail} and {@code inputs}
   */
  public static ImmutableExtensionMulti.Builder from(
      Extension.MultiRelDetail detail, List<Rel> inputs) {
    return ImmutableExtensionMulti.builder()
        .addAllInputs(inputs)
        .detail(detail)
        .deriveRecordType(detail.deriveRecordType(inputs));
  }

  /**
   * Creates a builder for {@link ExtensionMulti}.
   *
   * @return a new builder
   */
  public static ImmutableExtensionMulti.Builder builder() {
    return ImmutableExtensionMulti.builder();
  }
}
