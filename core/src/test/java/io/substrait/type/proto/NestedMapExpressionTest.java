package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ImmutableExpression;
import io.substrait.relation.Project;
import org.junit.jupiter.api.Test;

class NestedMapExpressionTest extends TestBase {
  Expression literalExpression = Expression.BoolLiteral.builder().value(true).build();
  Expression.ScalarFunctionInvocation nonLiteralExpression = sb.add(sb.i32(7), sb.i32(42));

  @Test
  void rejectEmptyNestedMap() {
    ImmutableExpression.NestedMap.Builder builder = Expression.NestedMap.builder();
    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void rejectNestedMapWithKeysOfDifferentTypes() {
    ImmutableExpression.NestedMap.Builder builder =
        Expression.NestedMap.builder()
            .putValues(sb.str("a"), literalExpression)
            .putValues(sb.i32(1), literalExpression);
    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void rejectNestedMapWithValuesOfDifferentTypes() {
    ImmutableExpression.NestedMap.Builder builder =
        Expression.NestedMap.builder()
            .putValues(sb.str("a"), literalExpression)
            .putValues(sb.str("b"), sb.i32(1));
    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void acceptNestedMapWithKeysAndValuesOfSameType() {
    ImmutableExpression.NestedMap.Builder builder =
        Expression.NestedMap.builder()
            .putValues(sb.str("a"), nonLiteralExpression)
            .putValues(sb.str("b"), sb.i32(12));
    assertDoesNotThrow(builder::build);

    verifyRoundTrip(projectOf(builder.build()));
  }

  @Test
  void literalNestedMapTest() {
    Expression.NestedMap literalNestedMap =
        Expression.NestedMap.builder()
            .putValues(sb.str("a"), literalExpression)
            .putValues(sb.str("b"), literalExpression)
            .build();

    verifyRoundTrip(projectOf(literalNestedMap));
  }

  @Test
  void literalNullableNestedMapTest() {
    Expression.NestedMap literalNestedMap =
        Expression.NestedMap.builder()
            .putValues(sb.str("a"), literalExpression)
            .putValues(sb.str("b"), literalExpression)
            .nullable(true)
            .build();

    verifyRoundTrip(projectOf(literalNestedMap));
  }

  @Test
  void nonLiteralNestedMapTest() {
    Expression.NestedMap nonLiteralNestedMap =
        Expression.NestedMap.builder()
            .putValues(nonLiteralExpression, nonLiteralExpression)
            .putValues(sb.i32(12), sb.i32(13))
            .build();

    verifyRoundTrip(projectOf(nonLiteralNestedMap));
  }

  @Test
  void nestedMapOfNestedMapsTest() {
    Expression.NestedMap inner =
        Expression.NestedMap.builder().putValues(sb.str("a"), sb.i32(1)).build();

    Expression.NestedMap outer =
        Expression.NestedMap.builder().putValues(sb.str("outer"), inner).build();

    verifyRoundTrip(projectOf(outer));
  }

  private Project projectOf(Expression expression) {
    return Project.builder().addExpressions(expression).input(sb.emptyVirtualTableScan()).build();
  }
}
