package io.substrait.relation;

import io.substrait.util.VisitationContext;
import org.immutables.value.Value;

/** A write relation whose behavior is supplied by an extension-defined detail object. */
@Value.Immutable
public abstract class ExtensionWrite extends AbstractWriteRel {
  /**
   * Returns the extension-specific detail describing this write relation.
   *
   * @return the extension detail object
   */
  public abstract Extension.WriteExtensionObject getDetail();

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a builder for {@link ExtensionWrite}.
   *
   * @return a new builder
   */
  public static ImmutableExtensionWrite.Builder builder() {
    return ImmutableExtensionWrite.builder();
  }
}
