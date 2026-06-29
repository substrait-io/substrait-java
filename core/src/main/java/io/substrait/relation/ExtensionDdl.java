package io.substrait.relation;

import io.substrait.util.VisitationContext;
import org.immutables.value.Value;

/** A DDL relation whose behavior is supplied by an extension-defined detail object. */
@Value.Immutable
public abstract class ExtensionDdl extends AbstractDdlRel implements HasExtension {
  /**
   * Returns the extension-specific detail describing this DDL relation.
   *
   * @return the extension detail object
   */
  public abstract Extension.DdlExtensionObject getDetail();

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a builder for {@link ExtensionDdl}.
   *
   * @return a new builder
   */
  public static ImmutableExtensionDdl.Builder builder() {
    return ImmutableExtensionDdl.builder();
  }
}
