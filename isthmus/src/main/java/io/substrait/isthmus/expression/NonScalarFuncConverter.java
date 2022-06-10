package io.substrait.isthmus.expression;

import io.substrait.expression.AbstractFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.type.Type;
import java.util.Optional;
import java.util.function.Function;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;

public interface NonScalarFuncConverter<
    T extends AbstractFunctionInvocation, R extends RexNode, E extends Expression> {
  public abstract Optional<T> convert(
      RelNode input, Type.Struct inputType, AggregateCall call, Function<R, E> topLevelConverter);
}
