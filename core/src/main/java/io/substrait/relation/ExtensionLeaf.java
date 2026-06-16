package io.substrait.relation;

import io.substrait.util.VisitationContext;
import org.immutables.value.Value;

/** A leaf relation whose behavior is supplied by an extension-defined detail object. */
@Value.Immutable
public abstract class ExtensionLeaf extends ZeroInputRel {

  /**
   * Returns the extension-specific detail describing this leaf relation.
   *
   * @return the extension detail object
   */
  public abstract Extension.LeafRelDetail getDetail();

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a builder initialized from the given extension detail.
   *
   * @param detail the extension detail describing the leaf relation
   * @return a builder populated from {@code detail}
   */
  public static ImmutableExtensionLeaf.Builder from(Extension.LeafRelDetail detail) {
    return ImmutableExtensionLeaf.builder()
        .detail(detail)
        .deriveRecordType(detail.deriveRecordType());
  }
}
