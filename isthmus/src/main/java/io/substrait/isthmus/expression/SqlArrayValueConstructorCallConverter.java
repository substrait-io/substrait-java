package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.isthmus.CallConverter;
import io.substrait.isthmus.TypeConverter;
import io.substrait.type.Type;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlArrayValueConstructor;

/** Converts Calcite {@link SqlArrayValueConstructor} calls into Substrait list literals. */
public class SqlArrayValueConstructorCallConverter implements CallConverter {

  private final TypeConverter typeConverter;

  /**
   * Creates a converter for array value constructors using the supplied {@link TypeConverter}.
   *
   * @param typeConverter Converter for Calcite element types to Substrait {@link Type}.
   */
  public SqlArrayValueConstructorCallConverter(TypeConverter typeConverter) {
    this.typeConverter = typeConverter;
  }

  /**
   * Attempts to convert a Calcite {@link RexCall} of {@link SqlArrayValueConstructor} into a
   * Substrait list expression.
   *
   * <p>Empty arrays are converted using {@link ExpressionCreator#emptyList(boolean, Type)} based on
   * the element type. Non-empty arrays are converted to a list of literals if all operands are
   * {@link Expression.Literal}, otherwise to a {@link Expression.NestedList}.
   *
   * @param call The Calcite array constructor call.
   * @param topLevelConverter Function converting {@link RexNode} operands to Substrait {@link
   *     Expression}s.
   * @return An {@link Optional} containing the converted {@link Expression} if the operator is
   *     {@link SqlArrayValueConstructor}; otherwise {@link Optional#empty()}.
   * @throws ClassCastException if non-empty operands are converted by {@code topLevelConverter}
   *     into non-literal expressions when a literal list is required.
   */
  @Override
  public Optional<Expression> convert(
      RexCall call, Function<RexNode, Expression> topLevelConverter) {
    SqlOperator operator = call.getOperator();
    if (operator instanceof SqlArrayValueConstructor) {
      return call.getOperands().isEmpty()
          ? toEmptyListLiteral(call)
          : toNonEmptyListLiteral(call, topLevelConverter);
    }
    return Optional.empty();
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
