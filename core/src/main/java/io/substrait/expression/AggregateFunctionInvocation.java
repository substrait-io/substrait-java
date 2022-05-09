package io.substrait.expression;

import io.substrait.function.SimpleExtension;
import io.substrait.type.Type;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
public abstract class AggregateFunctionInvocation {
  public abstract SimpleExtension.AggregateFunctionVariant declaration();

  public abstract List<Expression> arguments();

  public abstract Expression.AggregationPhase aggregationPhase();

  public abstract List<Expression.SortField> sort();

  public abstract Type outputType();

  public Type getType() {
    return outputType();
  }

  public static ImmutableAggregateFunctionInvocation.Builder builder() {
    return ImmutableAggregateFunctionInvocation.builder();
  }
}
