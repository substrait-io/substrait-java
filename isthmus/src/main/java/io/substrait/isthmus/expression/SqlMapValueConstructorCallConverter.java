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

/**
 * Converts Calcite {@link SqlMapValueConstructor} calls into Substrait map literals.
 *
 * <p>Expects an even-numbered operand list (key/value pairs) and produces an {@link Expression} map
 * literal via {@link ExpressionCreator}.
 */
public class SqlMapValueConstructorCallConverter implements CallConverter {

  SqlMapValueConstructorCallConverter() {}

  /**
   * Attempts to convert a Calcite {@link RexCall} representing a {@link SqlMapValueConstructor}
   * into a Substrait map literal.
   *
   * @param call The Calcite call to convert.
   * @param topLevelConverter Function for converting {@link RexNode} operands to Substrait {@link
   *     Expression}s.
   * @return An {@link Optional} containing the converted {@link Expression} if the operator is a
   *     {@link SqlMapValueConstructor}; otherwise {@link Optional#empty()}.
   * @throws ClassCastException if operands converted by {@code topLevelConverter} are not {@link
   *     Expression.Literal} instances.
   * @throws AssertionError if the number of operands is not even (expecting key/value pairs).
   */
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
