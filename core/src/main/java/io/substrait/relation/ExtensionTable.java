package io.substrait.relation;

import io.substrait.util.VisitationContext;
import org.immutables.value.Value;

/** A read relation backed by an extension-defined table detail object. */
@Value.Immutable
public abstract class ExtensionTable extends AbstractReadRel {

  /**
   * Returns the extension-specific detail describing this table.
   *
   * @return the extension detail object
   */
  public abstract Extension.ExtensionTableDetail getDetail();

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a builder initialized from the given extension detail.
   *
   * @param detail the extension detail describing the table
   * @return a builder populated from {@code detail}
   */
  public static ImmutableExtensionTable.Builder from(Extension.ExtensionTableDetail detail) {
    return ImmutableExtensionTable.builder().initialSchema(detail.deriveSchema()).detail(detail);
  }

  /**
   * Creates a builder for {@link ExtensionTable}.
   *
   * @return a new builder
   */
  public static ImmutableExtensionTable.Builder builder() {
    return ImmutableExtensionTable.builder();
  }
}
