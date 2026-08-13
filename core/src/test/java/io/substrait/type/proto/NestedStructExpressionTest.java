package io.substrait.type.proto;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import org.junit.jupiter.api.Test;

class NestedStructExpressionTest extends TestBase {
  Expression literalExpression = Expression.BoolLiteral.builder().value(true).build();
  Expression.ScalarFunctionInvocation nonLiteralExpression = sb.add(sb.i32(7), sb.i32(42));

  @Test
  void emptyNestedStructTest() {
    verifyRoundTrip(projectOf(Expression.NestedStruct.builder().build()));
  }

  @Test
  void literalNestedStructTest() {
    Expression.NestedStruct literalNestedStruct =
        Expression.NestedStruct.builder()
            .addFields(literalExpression)
            .addFields(sb.str("a"))
            .build();

    verifyRoundTrip(projectOf(literalNestedStruct));
  }

  @Test
  void literalNullableNestedStructTest() {
    Expression.NestedStruct literalNestedStruct =
        Expression.NestedStruct.builder().addFields(literalExpression).nullable(true).build();

    verifyRoundTrip(projectOf(literalNestedStruct));
  }

  @Test
  void heterogeneouslyTypedNestedStructTest() {
    Expression.NestedStruct nestedStruct =
        Expression.NestedStruct.builder()
            .addFields(nonLiteralExpression)
            .addFields(sb.str("a"))
            .addFields(literalExpression)
            .build();

    verifyRoundTrip(projectOf(nestedStruct));
  }

  @Test
  void nestedStructOfNestedTypesTest() {
    Expression.NestedStruct inner =
        Expression.NestedStruct.builder().addFields(sb.i32(1)).nullable(true).build();
    Expression.NestedList list =
        Expression.NestedList.builder().addValues(sb.i32(2)).addValues(sb.i32(3)).build();
    Expression.NestedMap map =
        Expression.NestedMap.builder()
            .addKeyValues(Expression.NestedMap.KeyValue.of(sb.str("a"), sb.i32(4)))
            .build();

    Expression.NestedStruct outer =
        Expression.NestedStruct.builder().addFields(inner).addFields(list).addFields(map).build();

    verifyRoundTrip(projectOf(outer));
  }
}
