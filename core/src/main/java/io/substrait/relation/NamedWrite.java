package io.substrait.relation;

import io.substrait.util.VisitationContext;
import java.util.List;
import org.immutables.value.Value;

/** A write relation that writes to a table identified by a multi-part name. */
@Value.Immutable
public abstract class NamedWrite extends AbstractWriteRel {
  /**
   * Returns the multi-part (namespaced) name identifying the table to write to.
   *
   * @return the table name components
   */
  public abstract List<String> getNames();

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a builder for {@link NamedWrite}.
   *
   * @return a new builder
   */
  public static ImmutableNamedWrite.Builder builder() {
    return ImmutableNamedWrite.builder();
  }
}
