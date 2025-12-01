package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.isthmus.CallConverter;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

public class NestedExpressionConverter implements CallConverter {

  public NestedExpressionConverter() {}

  @Override
  public Optional<Expression> convert(
      RexCall call, Function<RexNode, Expression> topLevelConverter) {

    if (!call.getOperator().getName().equals("nested_list")) {
      return Optional.empty();
    }

    List<Expression> values =
        call.operands.stream().map(topLevelConverter).collect(Collectors.toList());

    return Optional.of(
        Expression.NestedList.builder()
            .nullable(call.getType().isNullable())
            .values(values)
            .build());
  }
}
