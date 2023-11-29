package io.substrait.extendedexpression;

import io.substrait.expression.Expression;
import io.substrait.proto.AdvancedExtension;
import io.substrait.proto.AggregateFunction;
import io.substrait.type.NamedStruct;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public abstract class ExtendedExpression {
  public abstract List<ExpressionReference> getReferredExpressions();

  public abstract NamedStruct getBaseSchema();

  public abstract List<String> getExpectedTypeUrls();

  // creating simple extensions, such as extensionURIs and extensions, is performed on the fly

  public abstract Optional<AdvancedExtension> getAdvancedExtension();

  @Value.Immutable
  public abstract static class ExpressionReference {
    public abstract ExpressionTypeReference getExpressionType();

    public abstract List<String> getOutputNames();
  }

  public abstract static class ExpressionTypeReference {}

  @Value.Immutable
  public abstract static class ExpressionType extends ExpressionTypeReference {
    public abstract Expression getExpression();
  }

  @Value.Immutable
  public abstract static class AggregateFunctionType extends ExpressionTypeReference {
    public abstract AggregateFunction getMeasure();
  }
}
