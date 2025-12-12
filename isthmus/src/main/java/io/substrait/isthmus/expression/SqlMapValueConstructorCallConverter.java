package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.isthmus.CallConverter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlMapValueConstructor;

public class SqlMapValueConstructorCallConverter implements CallConverter {

  SqlMapValueConstructorCallConverter() {}

  @Override
  public Optional<Expression> convert(
      RexCall call, Function<RexNode, Expression> topLevelConverter) {
    SqlOperator operator = call.getOperator();
    if (operator instanceof SqlMapValueConstructor) {
      return toMapLiteral(call, topLevelConverter);
    }
    return Optional.empty();
  }

  private Optional<Expression> toMapLiteral(
      RexCall call, Function<RexNode, Expression> topLevelConverter) {
    List<Expression.Literal> literals =
        call.operands.stream()
            .map(t -> ((Expression.Literal) topLevelConverter.apply(t)))
            .collect(java.util.stream.Collectors.toList());
    Map<Expression.Literal, Expression.Literal> items = new HashMap<>();
    assert literals.size() % 2 == 0;
    for (int i = 0; i < literals.size(); i += 2) {
      items.put(literals.get(i), literals.get(i + 1));
    }
    return Optional.of(ExpressionCreator.map(false, items));
  }
}
