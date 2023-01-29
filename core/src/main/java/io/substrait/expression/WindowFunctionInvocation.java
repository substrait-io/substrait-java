package io.substrait.expression;

import io.substrait.function.SimpleExtension;
import io.substrait.proto.AggregateFunction;
import io.substrait.type.Type;
import java.util.List;
import java.util.Map;
import org.immutables.value.Value;

@Value.Immutable
public abstract class WindowFunctionInvocation {

  public abstract SimpleExtension.WindowFunctionVariant declaration();

  public abstract List<FunctionArg> arguments();

  public abstract Map<String, FunctionOption> options();

  public abstract Expression.AggregationPhase aggregationPhase();

  public abstract List<Expression.SortField> sort();

  public abstract Type outputType();

  public Type getType() {
    return outputType();
  }

  public abstract AggregateFunction.AggregationInvocation invocation();

  public static ImmutableWindowFunctionInvocation.Builder builder() {
    return ImmutableWindowFunctionInvocation.builder();
  }
}
