package io.substrait.relation.physical;

import io.substrait.expression.FieldReference;
import io.substrait.relation.RelVisitor;
import io.substrait.util.VisitationContext;
import java.util.List;
import org.immutables.value.Value;

/**
 * Exchange relation that scatters rows across destinations by hashing on a set of partitioning
 * fields.
 */
@Value.Immutable
public abstract class ScatterExchange extends AbstractExchangeRel {
  /**
   * Returns the fields used to determine each row's destination.
   *
   * @return the partitioning fields
   */
  public abstract List<FieldReference> getFields();

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a builder for {@link ScatterExchange}.
   *
   * @return a new builder
   */
  public static ImmutableScatterExchange.Builder builder() {
    return ImmutableScatterExchange.builder();
  }
}
