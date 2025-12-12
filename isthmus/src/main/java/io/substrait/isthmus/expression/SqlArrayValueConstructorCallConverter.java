package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.isthmus.CallConverter;
import io.substrait.isthmus.TypeConverter;
import io.substrait.type.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlArrayValueConstructor;
import org.apache.calcite.sql.fun.SqlMapValueConstructor;

public class SqlArrayValueConstructorCallConverter implements CallConverter {

  private final TypeConverter typeConverter;

  public SqlArrayValueConstructorCallConverter(TypeConverter typeConverter) {
    this.typeConverter = typeConverter;
  }

  @Override
  public Optional<Expression> convert(
      RexCall call, Function<RexNode, Expression> topLevelConverter) {
    SqlOperator operator = call.getOperator();
    if (operator instanceof SqlArrayValueConstructor) {
      return call.getOperands().isEmpty()
          ? toEmptyListLiteral(call)
          : toNonEmptyListLiteral(call, topLevelConverter);
    } else if (operator instanceof SqlMapValueConstructor) {
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

  private Optional<Expression> toNonEmptyListLiteral(
      RexCall call, Function<RexNode, Expression> topLevelConverter) {
    List<Expression> expressions =
        call.operands.stream().map(topLevelConverter).collect(Collectors.toList());

    // Check if all operands are actually literals
    if (expressions.stream().allMatch(e -> e instanceof Expression.Literal)) {
      return Optional.of(
          ExpressionCreator.list(
              call.getType().isNullable(),
              expressions.stream().map(e -> (Expression.Literal) e).collect(Collectors.toList())));
    } else {
      return Optional.of(
          Expression.NestedList.builder()
              .nullable(call.getType().isNullable())
              .values(expressions)
              .build());
    }
  }

  private Optional<Expression> toEmptyListLiteral(RexCall call) {
    RelDataType calciteElementType = call.getType().getComponentType();
    Type substraitElementType = typeConverter.toSubstrait(calciteElementType);
    return Optional.of(
        ExpressionCreator.emptyList(call.getType().isNullable(), substraitElementType));
  }
}
