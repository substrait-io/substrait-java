package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ImmutableExpression;
import org.junit.jupiter.api.Test;

class NestedListExpressionTest extends TestBase {
  io.substrait.expression.Expression literalExpression =
      Expression.BoolLiteral.builder().value(true).build();
  Expression.ScalarFunctionInvocation nonLiteralExpression = sb.add(sb.i32(7), sb.i32(42));

  @Test
  void rejectNestedListWithElementsOfDifferentTypes() {
    ImmutableExpression.NestedList.Builder builder =
        Expression.NestedList.builder().addValues(literalExpression).addValues(sb.i32(12));
    assertThrows(AssertionError.class, builder::build);
  }

  @Test
  void acceptNestedListWithElementsOfSameType() {
    ImmutableExpression.NestedList.Builder builder =
        Expression.NestedList.builder().addValues(nonLiteralExpression).addValues(sb.i32(12));
    assertDoesNotThrow(builder::build);

    io.substrait.relation.Project project =
        io.substrait.relation.Project.builder()
            .addExpressions(builder.build())
            .input(sb.emptyVirtualTableScan())
            .build();
    verifyRoundTrip(project);
  }

  @Test
  void rejectEmptyNestedListTest() {
    ImmutableExpression.NestedList.Builder builder = Expression.NestedList.builder();
    assertThrows(AssertionError.class, builder::build);
  }

  @Test
  void literalNestedListTest() {
    Expression.NestedList literalNestedList =
        Expression.NestedList.builder()
            .addValues(literalExpression)
            .addValues(literalExpression)
            .build();

    io.substrait.relation.Project project =
        io.substrait.relation.Project.builder()
            .addExpressions(literalNestedList)
            .input(sb.emptyVirtualTableScan())
            .build();

    verifyRoundTrip(project);
  }

  @Test
  void literalNullableNestedListTest() {
    Expression.NestedList literalNestedList =
        Expression.NestedList.builder()
            .addValues(literalExpression)
            .addValues(literalExpression)
            .nullable(true)
            .build();

    io.substrait.relation.Project project =
        io.substrait.relation.Project.builder()
            .addExpressions(literalNestedList)
            .input(sb.emptyVirtualTableScan())
            .build();

    verifyRoundTrip(project);
  }

  @Test
  void nonLiteralNestedListTest() {
    Expression.NestedList nonLiteralNestedList =
        Expression.NestedList.builder()
            .addValues(nonLiteralExpression)
            .addValues(nonLiteralExpression)
            .build();

    io.substrait.relation.Project project =
        io.substrait.relation.Project.builder()
            .addExpressions(nonLiteralNestedList)
            .input(sb.emptyVirtualTableScan())
            .build();

    verifyRoundTrip(project);
  }
}
