package io.substrait.expression;

import io.substrait.function.SimpleExtension;
import io.substrait.proto.AggregateFunction;
import io.substrait.type.Type;
import java.util.List;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Immutable
public abstract class AggregateFunctionInvocation {
  public abstract SimpleExtension.AggregateFunctionVariant declaration();

  public abstract List<FunctionArg> arguments();

  public final List<Expression> exprArguments() {
    return arguments().stream()
        .filter(f -> f instanceof Expression)
        .map(f -> Expression.class.cast(f))
        .collect(Collectors.toList());
  }

  public abstract Expression.AggregationPhase aggregationPhase();

  public abstract List<Expression.SortField> sort();

  public abstract Type outputType();

  public Type getType() {
    return outputType();
  }

  public abstract AggregateFunction.AggregationInvocation invocation();

  public static ImmutableAggregateFunctionInvocation.Builder builder() {
    return ImmutableAggregateFunctionInvocation.builder();
  }
}
