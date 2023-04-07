package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.type.Type;
import org.immutables.value.Value;

@Value.Immutable
public abstract class Filter extends SingleInputRel implements HasExtension {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Filter.class);

  public abstract Expression getCondition();

  @Override
  protected Type.Struct deriveRecordType() {
    return getInput().getRecordType();
  }

  @Override
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
  }

  public static ImmutableFilter.Builder builder() {
    return ImmutableFilter.builder();
  }
}
