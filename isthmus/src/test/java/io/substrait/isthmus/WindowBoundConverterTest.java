package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
    // Per the spec, a RANGE offset's type D must keep add(T, D) -> T defined for the ordering
    // expression's type T -- forcing it to int64 would break that for, e.g., an i32 column.
    RexNode offset = c(5, SqlTypeName.INTEGER);
    RexWindowBound bound = RexWindowBounds.preceding(offset);
    RelDataType orderingType = t(SqlTypeName.INTEGER);

    WindowBound converted =
        WindowBoundConverter.toWindowBound(
            bound, false, Optional.of(orderingType), rexExpressionConverter);

    assertEquals(WindowBound.Preceding.of(ExpressionCreator.i32(false, 5)), converted);
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
