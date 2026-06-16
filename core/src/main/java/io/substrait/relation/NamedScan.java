package io.substrait.relation;

import io.substrait.util.VisitationContext;
import java.util.List;
import org.immutables.value.Value;

/** A read relation that reads from a table identified by a multi-part name. */
@Value.Immutable
public abstract class NamedScan extends AbstractReadRel {

  /**
   * Returns the multi-part (namespaced) name identifying the table to read.
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
   * Creates a builder for {@link NamedScan}.
   *
   * @return a new builder
   */
  public static ImmutableNamedScan.Builder builder() {
    return ImmutableNamedScan.builder();
  }
}
