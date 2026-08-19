package io.substrait.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.type.TypeCreator;
import org.junit.jupiter.api.Test;

/**
 * Tests that literals carrying a fractional-second precision derive their own type class, with the
 * precision and nullability reaching the derived type.
 *
 * <p>Neither proto direction consults {@link Expression.Literal#getType()} — both work off the
 * literal's own fields — so a proto round trip cannot observe these derivations.
 */
class LiteralTypeDerivationTest {

  static final TypeCreator R = TypeCreator.REQUIRED;
  static final TypeCreator N = TypeCreator.NULLABLE;

  @Test
  void precisionTime() {
    assertEquals(R.precisionTime(6), ExpressionCreator.precisionTime(false, 42L, 6).getType());
    assertEquals(N.precisionTime(3), ExpressionCreator.precisionTime(true, 42L, 3).getType());
  }

  @Test
  void precisionTimestamp() {
    assertEquals(
        R.precisionTimestamp(6), ExpressionCreator.precisionTimestamp(false, 42L, 6).getType());
    assertEquals(
        N.precisionTimestamp(3), ExpressionCreator.precisionTimestamp(true, 42L, 3).getType());
  }

  @Test
  void precisionTimestampTZ() {
    assertEquals(
        R.precisionTimestampTZ(6), ExpressionCreator.precisionTimestampTZ(false, 42L, 6).getType());
    assertEquals(
        N.precisionTimestampTZ(3), ExpressionCreator.precisionTimestampTZ(true, 42L, 3).getType());
  }

  @Test
  void intervalDay() {
    assertEquals(R.intervalDay(6), ExpressionCreator.intervalDay(false, 1, 2, 3L, 6).getType());
    assertEquals(N.intervalDay(3), ExpressionCreator.intervalDay(true, 1, 2, 3L, 3).getType());
  }

  @Test
  void intervalCompound() {
    assertEquals(
        R.intervalCompound(6),
        ExpressionCreator.intervalCompound(false, 1, 2, 3, 4, 5L, 6).getType());
    assertEquals(
        N.intervalCompound(3),
        ExpressionCreator.intervalCompound(true, 1, 2, 3, 4, 5L, 3).getType());
  }
}
