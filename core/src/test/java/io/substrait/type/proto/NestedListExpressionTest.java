package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
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
    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void acceptNestedListWithElementsOfSameType() {
    ImmutableExpression.NestedList.Builder builder =
        Expression.NestedList.builder().addValues(nonLiteralExpression).addValues(sb.i32(12));
    assertDoesNotThrow(builder::build);

    verifyRoundTrip(projectOf(builder.build()));
  }

  @Test
  void acceptNestedListWithElementsOfMixedNullability() {
    // A list of values that differ only in nullability is valid; SQL builds such lists from
    // ARRAY[not_null_column, nullable_column].
    Expression.NestedList mixedNullability =
        Expression.NestedList.builder()
            .addValues(sb.i32(12))
            .addValues(ExpressionCreator.typedNull(N.I32))
            .build();
    // The element type is nullable, because the list holds a null.
    assertEquals(R.list(N.I32), mixedNullability.getType());
    // The value order does not change the type.
    assertEquals(
        mixedNullability.getType(),
        Expression.NestedList.builder()
            .addValues(ExpressionCreator.typedNull(N.I32))
            .addValues(sb.i32(12))
            .build()
            .getType());

    verifyRoundTrip(projectOf(mixedNullability));
  }

  @Test
  void rejectEmptyNestedListTest() {
    ImmutableExpression.NestedList.Builder builder = Expression.NestedList.builder();
    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void literalNestedListTest() {
    Expression.NestedList literalNestedList =
        Expression.NestedList.builder()
            .addValues(literalExpression)
            .addValues(literalExpression)
            .build();

    verifyRoundTrip(projectOf(literalNestedList));
  }

  @Test
  void literalNullableNestedListTest() {
    Expression.NestedList literalNestedList =
        Expression.NestedList.builder()
            .addValues(literalExpression)
            .addValues(literalExpression)
            .nullable(true)
            .build();

    verifyRoundTrip(projectOf(literalNestedList));
  }

  @Test
  void nonLiteralNestedListTest() {
    Expression.NestedList nonLiteralNestedList =
        Expression.NestedList.builder()
            .addValues(nonLiteralExpression)
            .addValues(nonLiteralExpression)
            .build();

    verifyRoundTrip(projectOf(nonLiteralNestedList));
  }
}
