package io.substrait.isthmus.expression;

import io.substrait.expression.WindowBound;
import java.math.BigDecimal;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Utility for converting Calcite {@link RexWindowBound} to Substrait {@link WindowBound}.
 *
 * <p>Supports {@code CURRENT ROW}, {@code UNBOUNDED}, and integer-offset {@code PRECEDING}/{@code
 * FOLLOWING} bounds.
 */
public class WindowBoundConverter {

  /**
   * Converts a Calcite {@link RexWindowBound} to a Substrait {@link WindowBound}.
   *
   * <p>Accepted forms:
   *
   * <ul>
   *   <li>{@code CURRENT ROW} → {@link WindowBound#CURRENT_ROW}
   *   <li>{@code UNBOUNDED} → {@link WindowBound#UNBOUNDED}
   *   <li>{@code PRECEDING n} / {@code FOLLOWING n} where {@code n} is an exact integer → {@link
   *       WindowBound.Preceding}/{@link WindowBound.Following}
   * </ul>
   *
   * @param rexWindowBound The Calcite window bound to convert.
   * @return The corresponding Substrait {@link WindowBound}.
   * @throws IllegalStateException if the bound is not one of CURRENT ROW, UNBOUNDED, PRECEDING, or
   *     FOLLOWING.
   * @throws IllegalArgumentException if the offset is not an exact integer type supported by
   *     Substrait.
   */
  public static WindowBound toWindowBound(RexWindowBound rexWindowBound) {
    if (rexWindowBound.isCurrentRow()) {
      return WindowBound.CURRENT_ROW;
    }
    if (rexWindowBound.isUnbounded()) {
      return WindowBound.UNBOUNDED;
    } else {
      RexNode node = rexWindowBound.getOffset();

      if (node instanceof RexLiteral) {
        RexLiteral literal = (RexLiteral) node;
        if (SqlTypeName.EXACT_TYPES.contains(literal.getTypeName())) {
          BigDecimal offset = (BigDecimal) literal.getValue4();

          if (rexWindowBound.isPreceding()) {
            return WindowBound.Preceding.of(offset.longValue());
          }
          if (rexWindowBound.isFollowing()) {
            return WindowBound.Following.of(offset.longValue());
          }

          throw new IllegalStateException(
              "window bound was none of CURRENT ROW, UNBOUNDED, PRECEDING or FOLLOWING");
        }
      }

      throw new IllegalArgumentException(
          String.format(
              "substrait only supports integer window offsets. Received: %s",
              rexWindowBound.getOffset().getKind()));
    }
  }
}
