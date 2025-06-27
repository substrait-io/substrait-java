package io.substrait.extendedexpression;

import io.substrait.expression.Expression;
import io.substrait.proto.AdvancedExtension;
import io.substrait.relation.Aggregate;
import io.substrait.type.NamedStruct;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public abstract class ExtendedExpression {
  public abstract List<ExpressionReferenceBase> getReferredExpressions();

  public abstract NamedStruct getBaseSchema();

  public abstract List<String> getExpectedTypeUrls();

  // creating simple extensions, such as extensionURIs and extensions, is performed on the fly

  public abstract Optional<AdvancedExtension> getAdvancedExtension();

  public static ImmutableExtendedExpression.Builder builder() {
    return ImmutableExtendedExpression.builder();
  }

  public interface ExpressionReferenceBase {
    List<String> getOutputNames();
  }

  @Value.Immutable
  public abstract static class ExpressionReference implements ExpressionReferenceBase {
    public abstract Expression getExpression();

    public static ImmutableExpressionReference.Builder builder() {
      return ImmutableExpressionReference.builder();
    }
  }

  @Value.Immutable
  public abstract static class AggregateFunctionReference implements ExpressionReferenceBase {
    public abstract Aggregate.Measure getMeasure();
  }
}
