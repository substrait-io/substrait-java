package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.isthmus.CallConverter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlMapValueConstructor;

/**
 * Converts Calcite {@link SqlMapValueConstructor} calls into Substrait map expressions.
 *
 * <p>Expects an even-numbered operand list (key/value pairs) and produces an {@link
 * Expression.MapLiteral} when every key and value is a literal, and an {@link Expression.NestedMap}
 * otherwise. Either way the map takes its nullability from the call's type, which is where Calcite
 * keeps it: on a Substrait literal, {@code nullable} marks the literal's type as nullable rather
 * than the value as null, so a nullable map of literals is still a MapLiteral.
 */
public class SqlMapValueConstructorCallConverter implements CallConverter {

  /** Default constructor. */
  public SqlMapValueConstructorCallConverter() {}

  /**
   * Attempts to convert a Calcite {@link RexCall} representing a {@link SqlMapValueConstructor}
   * into a Substrait map expression.
   *
   * @param call The Calcite call to convert.
   * @param topLevelConverter Function for converting {@link RexNode} operands to Substrait {@link
   *     Expression}s.
   * @return An {@link Optional} containing the converted {@link Expression} if the operator is a
   *     {@link SqlMapValueConstructor}; otherwise {@link Optional#empty()}.
   * @throws IllegalArgumentException if the number of operands is not even (expecting key/value
   *     pairs).
   */
  @Override
  public Optional<Expression> convert(
      RexCall call, Function<RexNode, Expression> topLevelConverter) {
    SqlOperator operator = call.getOperator();
    if (operator instanceof SqlMapValueConstructor) {
      return toMap(call, topLevelConverter);
    }
    return Optional.empty();
  }

  private Optional<Expression> toMap(
      RexCall call, Function<RexNode, Expression> topLevelConverter) {
    if (call.operands.size() % 2 != 0) {
      throw new IllegalArgumentException(
          String.format(
              "A map value constructor takes key/value pairs, so it must have an even number of"
                  + " operands, but it has %d.",
              call.operands.size()));
    }

    List<Expression> expressions =
        call.operands.stream().map(topLevelConverter).collect(Collectors.toList());

    // The maps below are LinkedHashMaps so that the pairs keep the order they were written in.
    if (expressions.stream().allMatch(e -> e instanceof Expression.Literal)) {
      Map<Expression.Literal, Expression.Literal> literals = new LinkedHashMap<>();
      for (int i = 0; i < expressions.size(); i += 2) {
        literals.put(
            (Expression.Literal) expressions.get(i), (Expression.Literal) expressions.get(i + 1));
      }
      return Optional.of(ExpressionCreator.map(call.getType().isNullable(), literals));
    }

    Map<Expression, Expression> values = new LinkedHashMap<>();
    for (int i = 0; i < expressions.size(); i += 2) {
      values.put(expressions.get(i), expressions.get(i + 1));
    }
    return Optional.of(ExpressionCreator.nestedMap(call.getType().isNullable(), values));
  }
}
