package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.junit.jupiter.api.Test;

class DynamicParameterRoundtripTest extends TestBase {

  @Test
  void dynamicParameterI64() {
    Expression.DynamicParameter dp =
        Expression.DynamicParameter.builder()
            .type(TypeCreator.REQUIRED.I64)
            .parameterReference(0)
            .build();

    assertDynamicParameter(dp, TypeCreator.REQUIRED.I64, 0);
    verifyRoundTrip(dp);
  }

  @Test
  void dynamicParameterNullableString() {
    Expression.DynamicParameter dp =
        Expression.DynamicParameter.builder()
            .type(TypeCreator.NULLABLE.STRING)
            .parameterReference(1)
            .build();

    assertDynamicParameter(dp, TypeCreator.NULLABLE.STRING, 1);
    verifyRoundTrip(dp);
  }

  @Test
  void dynamicParameterFP64() {
    Expression.DynamicParameter dp =
        Expression.DynamicParameter.builder()
            .type(TypeCreator.REQUIRED.FP64)
            .parameterReference(2)
            .build();

    assertDynamicParameter(dp, TypeCreator.REQUIRED.FP64, 2);
    verifyRoundTrip(dp);
  }

  @Test
  void dynamicParameterI32Nullable() {
    Expression.DynamicParameter dp =
        Expression.DynamicParameter.builder()
            .type(TypeCreator.NULLABLE.I32)
            .parameterReference(42)
            .build();

    assertDynamicParameter(dp, TypeCreator.NULLABLE.I32, 42);
    verifyRoundTrip(dp);
  }

  @Test
  void dynamicParameterDate() {
    Expression.DynamicParameter dp =
        Expression.DynamicParameter.builder()
            .type(TypeCreator.REQUIRED.DATE)
            .parameterReference(3)
            .build();

    assertDynamicParameter(dp, TypeCreator.REQUIRED.DATE, 3);
    verifyRoundTrip(dp);
  }

  @Test
  void dynamicParameterBoolean() {
    Expression.DynamicParameter dp =
        Expression.DynamicParameter.builder()
            .type(TypeCreator.REQUIRED.BOOLEAN)
            .parameterReference(0)
            .build();

    assertDynamicParameter(dp, TypeCreator.REQUIRED.BOOLEAN, 0);
    verifyRoundTrip(dp);
  }

  @Test
  void dynamicParameterDecimal() {
    Expression.DynamicParameter dp =
        Expression.DynamicParameter.builder()
            .type(TypeCreator.REQUIRED.decimal(10, 2))
            .parameterReference(5)
            .build();

    assertDynamicParameter(dp, TypeCreator.REQUIRED.decimal(10, 2), 5);
    verifyRoundTrip(dp);
  }

  @Test
  void dynamicParameterTimestamp() {
    Expression.DynamicParameter dp =
        Expression.DynamicParameter.builder()
            .type(TypeCreator.NULLABLE.TIMESTAMP)
            .parameterReference(7)
            .build();

    assertDynamicParameter(dp, TypeCreator.NULLABLE.TIMESTAMP, 7);
    verifyRoundTrip(dp);
  }

  private void assertDynamicParameter(
      Expression.DynamicParameter dp, Type expectedType, int expectedReference) {
    assertEquals(expectedType, dp.getType());
    assertEquals(expectedReference, dp.parameterReference());
  }
}
