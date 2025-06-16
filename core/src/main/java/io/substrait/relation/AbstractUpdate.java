package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import java.util.List;
import org.immutables.value.Value;

public abstract class AbstractUpdate extends ZeroInputRel implements HasExtension {

  public abstract NamedStruct getTableSchema();

  public abstract Expression getCondition();

  public abstract List<TransformExpression> getTransformations();

  @Value.Immutable
  public abstract static class TransformExpression {
    public abstract Expression getTransformation();

    public abstract int getColumnTarget();

    public static ImmutableTransformExpression.Builder builder() {
      return ImmutableTransformExpression.builder();
    }
  }

  @Override
  public Type.Struct deriveRecordType() {
    return getTableSchema().struct();
  }
}
