package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.isthmus.CallConverter;
import java.util.ArrayList;
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
 * Expression.MapLiteral} when every key and value is a literal and no key is repeated, and an
 * {@link Expression.NestedMap} otherwise. A MapLiteral holds its pairs in a {@code Map}, so a
 * repeated key would cost a pair; a NestedMap keeps them in a list and can represent it.
 *
 * <p>Either way the map takes its nullability from the call's type, which is where Calcite keeps
 * it: on a Substrait literal, {@code nullable} marks the literal's type as nullable rather than the
 * value as null, so a nullable map of literals is still a MapLiteral.
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
              "SqlMapValueConstructor requires an even number of operands (key/value pairs), but got"
                  + " %d",
              call.operands.size()));
    }

    List<Expression> expressions =
        call.operands.stream().map(topLevelConverter).collect(Collectors.toList());

    List<Expression.NestedMap.KeyValue> keyValues = new ArrayList<>();
    for (int i = 0; i < expressions.size(); i += 2) {
      keyValues.add(Expression.NestedMap.KeyValue.of(expressions.get(i), expressions.get(i + 1)));
    }

    // A MapLiteral holds its pairs in a Map, so it can only stand in for this call when every key
    // and value is a literal and no key is repeated - a repeated key would lose a pair.
    boolean allLiterals = expressions.stream().allMatch(e -> e instanceof Expression.Literal);
    boolean keysAreDistinct =
        keyValues.stream().map(Expression.NestedMap.KeyValue::key).distinct().count()
            == keyValues.size();
    if (allLiterals && keysAreDistinct) {
      // A LinkedHashMap so that the pairs keep the order they were written in.
      Map<Expression.Literal, Expression.Literal> literals = new LinkedHashMap<>();
      for (Expression.NestedMap.KeyValue keyValue : keyValues) {
        literals.put((Expression.Literal) keyValue.key(), (Expression.Literal) keyValue.value());
      }
      return Optional.of(ExpressionCreator.map(call.getType().isNullable(), literals));
    }

    return Optional.of(ExpressionCreator.nestedMap(call.getType().isNullable(), keyValues));
  }
}
