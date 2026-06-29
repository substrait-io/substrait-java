package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.type.Type;
import io.substrait.util.VisitationContext;
import java.util.List;
import org.immutables.value.Value;

/** A relation that orders its input rows according to a list of sort fields. */
@Value.Immutable
public abstract class Sort extends SingleInputRel implements HasExtension {

  /**
   * Returns the sort fields defining the ordering, applied in order of precedence.
   *
   * @return the sort fields
   */
  public abstract List<Expression.SortField> getSortFields();

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
   * Creates a builder for {@link Sort}.
   *
   * @return a new builder
   */
  public static ImmutableSort.Builder builder() {
    return ImmutableSort.builder();
  }
}
