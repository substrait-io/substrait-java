package io.substrait.type.proto;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class NestedListExpressionTest extends TestBase {
  io.substrait.expression.Expression literalExpression =
      Expression.BoolLiteral.builder().value(true).build();
  Expression.ScalarFunctionInvocation nonLiteralExpression = b.add(b.i32(7), b.i32(42));

  @Test
  void literalNestedListTest() {
    List<Expression> expressionList = new ArrayList<>();
    Expression.NestedList literalNestedList =
        Expression.NestedList.builder()
            .addValues(literalExpression)
            .addValues(literalExpression)
            .build();
    expressionList.add(literalNestedList);

    io.substrait.relation.Project project =
        io.substrait.relation.Project.builder()
            .expressions(expressionList)
            .input(b.emptyScan())
            .build();

    verifyRoundTrip(project);
  }

  @Test
  void nonLiteralNestedListTest() {
    List<Expression> expressionList = new ArrayList<>();

    Expression.NestedList nonLiteralNestedList =
        Expression.NestedList.builder()
            .addValues(nonLiteralExpression)
            .addValues(nonLiteralExpression)
            .build();
    expressionList.add(nonLiteralNestedList);

    io.substrait.relation.Project project =
        io.substrait.relation.Project.builder()
            .expressions(expressionList)
            .input(b.emptyScan())
            .build();

    verifyRoundTrip(project);
  }
}
