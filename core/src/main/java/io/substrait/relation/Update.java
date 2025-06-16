package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
public abstract class Update extends ZeroInputRel implements HasExtension {

  public abstract List<String> getNames();

  public abstract NamedStruct getTableSchema();

  public abstract Expression getCondition();

  public abstract List<TransformExpression> getTransformations();

  @Override
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
  }

  @Value.Immutable
  public abstract static class TransformExpression {
    public abstract Expression getTransformation();

    public abstract int getColumnTarget();

    public static ImmutableTransformExpression.Builder builder() {
      return ImmutableTransformExpression.builder();
    }
  }

  public static ImmutableUpdate.Builder builder() {
    return ImmutableUpdate.builder();
  }

  @Override
  public Type.Struct deriveRecordType() {
    return getTableSchema().struct();
  }
}
