package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.type.Type;
import io.substrait.util.VisitationContext;
import org.immutables.value.Value;

/** A relation that retains only the input rows for which a boolean condition evaluates to true. */
@Value.Immutable
public abstract class Filter extends SingleInputRel implements HasExtension {

  /**
   * Returns the boolean condition used to filter input rows.
   *
   * @return the filter condition
   */
  public abstract Expression getCondition();

  @Override
  protected Type.Struct deriveRecordType() {
    return getInput().getRecordType();
  }

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a builder for {@link Filter}.
   *
   * @return a new builder
   */
  public static ImmutableFilter.Builder builder() {
    return ImmutableFilter.builder();
  }
}
