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
  Expression.ScalarFunctionInvocation nonLiteralExpression = b.add(b.i32(7), b.i32(42));

  @Test
  void DifferentTypedLiteralsNestedListTest() {
    ImmutableExpression.NestedList.Builder builder =
        Expression.NestedList.builder().addValues(literalExpression).addValues(b.i32(12));
    assertThrows(AssertionError.class, builder::build);
  }

  @Test
  void SameTypedLiteralsNestedListTest() {
    ImmutableExpression.NestedList.Builder builder =
        Expression.NestedList.builder().addValues(nonLiteralExpression).addValues(b.i32(12));
    assertDoesNotThrow(builder::build);

    io.substrait.relation.Project project =
        io.substrait.relation.Project.builder()
            .addExpressions(builder.build())
            .input(b.emptyScan())
            .build();
    verifyRoundTrip(project);
  }

  @Test
  void EmptyListNestedListTest() {
    ImmutableExpression.NestedList emptyNestedList = Expression.NestedList.builder().build();

    io.substrait.relation.Project project =
        io.substrait.relation.Project.builder()
            .addExpressions(emptyNestedList)
            .input(b.emptyScan())
            .build();
    verifyRoundTrip(project);
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
            .input(b.emptyScan())
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
            .input(b.emptyScan())
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
            .input(b.emptyScan())
            .build();

    verifyRoundTrip(project);
  }
}
