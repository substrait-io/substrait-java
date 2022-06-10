package io.substrait.expression;

import io.substrait.function.SimpleExtension;
import io.substrait.type.Type;
import java.util.List;

public abstract class AbstractFunctionInvocation<T extends SimpleExtension.Function, I> {

  public abstract T declaration();

  public abstract List<Expression> arguments();

  public abstract Expression.AggregationPhase aggregationPhase();

  public abstract List<Expression.SortField> sort();

  public abstract Type outputType();

  public Type getType() {
    return outputType();
  }

  public abstract I invocation();
}
