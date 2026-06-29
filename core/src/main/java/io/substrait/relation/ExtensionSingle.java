package io.substrait.relation;

import io.substrait.util.VisitationContext;
import org.immutables.value.Value;

/** A single-input relation whose behavior is supplied by an extension-defined detail object. */
@Value.Immutable
public abstract class ExtensionSingle extends SingleInputRel {

  /**
   * Returns the extension-specific detail describing this relation.
   *
   * @return the extension detail object
   */
  public abstract Extension.SingleRelDetail getDetail();

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a builder initialized from the given extension detail and input relation.
   *
   * @param detail the extension detail describing the relation
   * @param input the input relation
   * @return a builder populated from {@code detail} and {@code input}
   */
  public static ImmutableExtensionSingle.Builder from(Extension.SingleRelDetail detail, Rel input) {
    return ImmutableExtensionSingle.builder()
        .input(input)
        .detail(detail)
        .deriveRecordType(detail.deriveRecordType(input));
  }

  /**
   * Creates a builder for {@link ExtensionSingle}.
   *
   * @return a new builder
   */
  public static ImmutableExtensionSingle.Builder builder() {
    return ImmutableExtensionSingle.builder();
  }
}
