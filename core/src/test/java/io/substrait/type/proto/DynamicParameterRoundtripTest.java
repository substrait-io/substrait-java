package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
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

    assertEquals(TypeCreator.REQUIRED.I64, dp.getType());
    verifyRoundTrip(dp);
  }

  @Test
  void dynamicParameterNullableString() {
    Expression.DynamicParameter dp =
        Expression.DynamicParameter.builder()
            .type(TypeCreator.NULLABLE.STRING)
            .parameterReference(1)
            .build();

    assertEquals(TypeCreator.NULLABLE.STRING, dp.getType());
    verifyRoundTrip(dp);
  }

  @Test
  void dynamicParameterFP64() {
    Expression.DynamicParameter dp =
        Expression.DynamicParameter.builder()
            .type(TypeCreator.REQUIRED.FP64)
            .parameterReference(2)
            .build();

    assertEquals(TypeCreator.REQUIRED.FP64, dp.getType());
    verifyRoundTrip(dp);
  }

  @Test
  void dynamicParameterI32Nullable() {
    Expression.DynamicParameter dp =
        Expression.DynamicParameter.builder()
            .type(TypeCreator.NULLABLE.I32)
            .parameterReference(42)
            .build();

    assertEquals(42, dp.parameterReference());
    verifyRoundTrip(dp);
  }

  @Test
  void dynamicParameterDate() {
    Expression.DynamicParameter dp =
        Expression.DynamicParameter.builder()
            .type(TypeCreator.REQUIRED.DATE)
            .parameterReference(3)
            .build();

    assertEquals(TypeCreator.REQUIRED.DATE, dp.getType());
    verifyRoundTrip(dp);
  }

  @Test
  void dynamicParameterBoolean() {
    Expression.DynamicParameter dp =
        Expression.DynamicParameter.builder()
            .type(TypeCreator.REQUIRED.BOOLEAN)
            .parameterReference(0)
            .build();

    assertEquals(TypeCreator.REQUIRED.BOOLEAN, dp.getType());
    verifyRoundTrip(dp);
  }

  @Test
  void dynamicParameterDecimal() {
    Expression.DynamicParameter dp =
        Expression.DynamicParameter.builder()
            .type(TypeCreator.REQUIRED.decimal(10, 2))
            .parameterReference(5)
            .build();

    assertEquals(TypeCreator.REQUIRED.decimal(10, 2), dp.getType());
    verifyRoundTrip(dp);
  }

  @Test
  void dynamicParameterTimestamp() {
    Expression.DynamicParameter dp =
        Expression.DynamicParameter.builder()
            .type(TypeCreator.NULLABLE.TIMESTAMP)
            .parameterReference(7)
            .build();

    assertEquals(TypeCreator.NULLABLE.TIMESTAMP, dp.getType());
    verifyRoundTrip(dp);
  }
}
