package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.type.Type;
import io.substrait.util.VisitationContext;
import org.immutables.value.Value;

@Value.Immutable
public abstract class Filter extends SingleInputRel implements HasExtension {

  public abstract Expression getCondition();

  @Override
  protected Type.Struct deriveRecordType() {
    return getInput().getRecordType();
  }

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      final RelVisitor<O, C, E> visitor, final C context) throws E {
    return visitor.visit(this, context);
  }

  public static ImmutableFilter.Builder builder() {
    return ImmutableFilter.builder();
  }
}
