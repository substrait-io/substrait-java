package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.type.TypeCreator;
import org.junit.jupiter.api.Test;

class ExecutionContextVariableRoundtripTest extends TestBase {

  @Test
  void currentTimestampMicros() {
    Expression.CurrentTimestamp expr = Expression.CurrentTimestamp.builder().precision(6).build();

    assertEquals(TypeCreator.REQUIRED.precisionTimestampTZ(6), expr.getType());
    verifyRoundTrip(expr);
  }

  @Test
  void currentTimestampSeconds() {
    Expression.CurrentTimestamp expr = Expression.CurrentTimestamp.builder().precision(0).build();

    assertEquals(TypeCreator.REQUIRED.precisionTimestampTZ(0), expr.getType());
    verifyRoundTrip(expr);
  }

  @Test
  void currentTimezone() {
    Expression.CurrentTimezone expr = Expression.CurrentTimezone.builder().build();

    assertEquals(TypeCreator.REQUIRED.STRING, expr.getType());
    verifyRoundTrip(expr);
  }

  @Test
  void currentDate() {
    Expression.CurrentDate expr = Expression.CurrentDate.builder().build();

    assertEquals(TypeCreator.REQUIRED.DATE, expr.getType());
    verifyRoundTrip(expr);
  }
}
