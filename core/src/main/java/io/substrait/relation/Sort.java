package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.type.Type;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
public abstract class Sort extends SingleInputRel implements HasExtension {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Sort.class);

  public abstract List<Expression.SortField> getSortFields();

  @Override
  protected Type.Struct deriveRecordType() {
    return getInput().getRecordType();
  }

  @Override
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
  }

  public static ImmutableSort.Builder builder() {
    return ImmutableSort.builder();
  }
}
