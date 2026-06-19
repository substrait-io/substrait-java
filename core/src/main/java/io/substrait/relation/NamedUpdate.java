package io.substrait.relation;

import io.substrait.util.VisitationContext;
import java.util.List;
import org.immutables.value.Value;

/** An update relation that targets a table identified by a multi-part name. */
@Value.Immutable
public abstract class NamedUpdate extends AbstractUpdate {

  /**
   * Returns the multi-part (namespaced) name identifying the table to update.
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
   * Creates a builder for {@link NamedUpdate}.
   *
   * @return a new builder
   */
  public static ImmutableNamedUpdate.Builder builder() {
    return ImmutableNamedUpdate.builder();
  }
}
