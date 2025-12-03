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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlArrayValueConstructor;
import org.apache.calcite.sql.fun.SqlMapValueConstructor;

public class LiteralConstructorConverter implements CallConverter {

  private final TypeConverter typeConverter;

  public LiteralConstructorConverter(final TypeConverter typeConverter) {
    this.typeConverter = typeConverter;
  }

  @Override
  public Optional<Expression> convert(
      final RexCall call, final Function<RexNode, Expression> topLevelConverter) {
    final SqlOperator operator = call.getOperator();
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
      final RexCall call, final Function<RexNode, Expression> topLevelConverter) {
    final List<Expression.Literal> literals =
        call.operands.stream()
            .map(t -> ((Expression.Literal) topLevelConverter.apply(t)))
            .collect(java.util.stream.Collectors.toList());
    final Map<Expression.Literal, Expression.Literal> items = new HashMap<>();
    assert literals.size() % 2 == 0;
    for (int i = 0; i < literals.size(); i += 2) {
      items.put(literals.get(i), literals.get(i + 1));
    }
    return Optional.of(ExpressionCreator.map(false, items));
  }

  private Optional<Expression> toNonEmptyListLiteral(
      final RexCall call, final Function<RexNode, Expression> topLevelConverter) {
    return Optional.of(
        ExpressionCreator.list(
            call.getType().isNullable(),
            call.operands.stream()
                .map(t -> ((Expression.Literal) topLevelConverter.apply(t)))
                .collect(java.util.stream.Collectors.toList())));
  }

  private Optional<Expression> toEmptyListLiteral(final RexCall call) {
    final RelDataType calciteElementType = call.getType().getComponentType();
    final Type substraitElementType = typeConverter.toSubstrait(calciteElementType);
    return Optional.of(
        ExpressionCreator.emptyList(call.getType().isNullable(), substraitElementType));
  }
}
