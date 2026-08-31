package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.WindowBound;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.isthmus.expression.WindowBoundConverter;
import java.math.BigDecimal;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

class WindowBoundConverterTest extends CalciteObjs {

  private final RexExpressionConverter rexExpressionConverter = new RexExpressionConverter();

  @Test
  void currentRow() {
    assertEquals(
        WindowBound.CURRENT_ROW,
        WindowBoundConverter.toWindowBound(
            RexWindowBounds.CURRENT_ROW, true, Optional.empty(), rexExpressionConverter));
  }

  @Test
  void unbounded() {
    assertEquals(
        WindowBound.UNBOUNDED,
        WindowBoundConverter.toWindowBound(
            RexWindowBounds.UNBOUNDED_PRECEDING, true, Optional.empty(), rexExpressionConverter));
    assertEquals(
        WindowBound.UNBOUNDED,
        WindowBoundConverter.toWindowBound(
            RexWindowBounds.UNBOUNDED_FOLLOWING, true, Optional.empty(), rexExpressionConverter));
  }

  @Test
  void precedingWithDecimalOffsetKeepsItsFraction() {
    // Regression test: DECIMAL is in SqlTypeName.EXACT_TYPES, so a naive `.longValue()`
    // truncation would silently drop the fractional part instead of preserving it.
    RexNode offset = c(new BigDecimal("10.5"), SqlTypeName.DECIMAL, 19, 1);
    RexWindowBound bound = RexWindowBounds.preceding(offset);

    WindowBound converted =
        WindowBoundConverter.toWindowBound(bound, false, Optional.empty(), rexExpressionConverter);

    assertEquals(
        WindowBound.Preceding.of(ExpressionCreator.decimal(false, new BigDecimal("10.5"), 19, 1)),
        converted);
  }

  @Test
  void followingWithNonLiteralOffset() {
    // A non-literal offset must round trip as an expression rather than be rejected.
    RexNode offset = rex.makeInputRef(t(SqlTypeName.BIGINT), 0);
    RexWindowBound bound = RexWindowBounds.following(offset);

    WindowBound converted =
        WindowBoundConverter.toWindowBound(bound, false, Optional.empty(), rexExpressionConverter);

    Expression expectedOffset = offset.accept(rexExpressionConverter);
    assertEquals(WindowBound.Following.of(expectedOffset), converted);
  }

  @Test
  void rowsIntegralOffsetIsWidenedToI64() {
    // Per the spec, a BOUNDS_TYPE_ROWS offset_expr's type must be int64, regardless of the
    // literal's natural Calcite width.
    RexNode offset = c(5, SqlTypeName.INTEGER);
    RexWindowBound bound = RexWindowBounds.preceding(offset);

    WindowBound converted =
        WindowBoundConverter.toWindowBound(bound, true, Optional.empty(), rexExpressionConverter);

    assertEquals(WindowBound.Preceding.of(ExpressionCreator.i64(false, 5)), converted);
  }

  @Test
  void rangeIntegralOffsetTakesTheOrderingExpressionType() {
    // isthmus requires a RANGE offset's type to exactly match the ordering expression's type T --
    // forcing it to int64 would break that for, e.g., an i32 column.
    RexNode offset = c(5, SqlTypeName.INTEGER);
    RexWindowBound bound = RexWindowBounds.preceding(offset);
    RelDataType orderingType = t(SqlTypeName.INTEGER);

    WindowBound converted =
        WindowBoundConverter.toWindowBound(
            bound, false, Optional.of(orderingType), rexExpressionConverter);

    assertEquals(WindowBound.Preceding.of(ExpressionCreator.i32(false, 5)), converted);
  }

  @Test
  void rangeOffsetOutOfRangeForOrderingTypeThrows() {
    // Calcite's SqlWindow#validateFrameBoundary only checks the bound's type family against the
    // ordering type for RANGE, not its range, so an offset that doesn't fit the ordering column's
    // narrower type must be rejected here rather than silently kept as the literal's own type.
    RexNode offset = c(100000, SqlTypeName.INTEGER);
    RexWindowBound bound = RexWindowBounds.preceding(offset);
    RelDataType orderingType = t(SqlTypeName.SMALLINT);

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            WindowBoundConverter.toWindowBound(
                bound, false, Optional.of(orderingType), rexExpressionConverter));
  }

  @Test
  void rangeOffsetExceedingDecimalPrecisionThrows() {
    RexNode offset = c(12345, SqlTypeName.INTEGER);
    RexWindowBound bound = RexWindowBounds.preceding(offset);
    RelDataType orderingType = t(SqlTypeName.DECIMAL, 5, 2);

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            WindowBoundConverter.toWindowBound(
                bound, false, Optional.of(orderingType), rexExpressionConverter));
  }

  @Test
  void rangeOffsetFailingFloatRoundTripThrows() {
    // 16_777_217 (2^24 + 1) is the first integer a 24-bit float mantissa cannot represent exactly.
    RexNode offset = c(16777217, SqlTypeName.INTEGER);
    RexWindowBound bound = RexWindowBounds.preceding(offset);
    RelDataType orderingType = t(SqlTypeName.REAL);

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            WindowBoundConverter.toWindowBound(
                bound, false, Optional.of(orderingType), rexExpressionConverter));
  }

  @Test
  void rangeOffsetAgainstUnsupportedOrderingTypeThrows() {
    // integralLiteralOfType has no case for a temporal ordering column, so no non-zero offset can
    // ever be retyped to it.
    RexNode offset = c(5, SqlTypeName.INTEGER);
    RexWindowBound bound = RexWindowBounds.preceding(offset);
    RelDataType orderingType = t(SqlTypeName.TIMESTAMP);

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            WindowBoundConverter.toWindowBound(
                bound, false, Optional.of(orderingType), rexExpressionConverter));
  }

  @Test
  void zeroOffsetBecomesCurrentRow() {
    // Per the spec, zero is not a valid offset and is equivalent to CurrentRow; producers should
    // emit CurrentRow rather than a zero offset_expr.
    RexNode offset = c(0, SqlTypeName.INTEGER);
    RexWindowBound bound = RexWindowBounds.preceding(offset);

    WindowBound converted =
        WindowBoundConverter.toWindowBound(bound, true, Optional.empty(), rexExpressionConverter);

    assertEquals(WindowBound.CURRENT_ROW, converted);
  }
}
