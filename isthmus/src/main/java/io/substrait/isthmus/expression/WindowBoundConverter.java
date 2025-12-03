package io.substrait.isthmus.expression;

import io.substrait.expression.WindowBound;
import java.math.BigDecimal;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.type.SqlTypeName;

public class WindowBoundConverter {

  /** Converts a {@link RexWindowBound} to a {@link WindowBound}. */
  public static WindowBound toWindowBound(final RexWindowBound rexWindowBound) {
    if (rexWindowBound.isCurrentRow()) {
      return WindowBound.CURRENT_ROW;
    }
    if (rexWindowBound.isUnbounded()) {
      return WindowBound.UNBOUNDED;
    } else {
      final RexNode node = rexWindowBound.getOffset();

      if (node instanceof RexLiteral) {
        final RexLiteral literal = (RexLiteral) node;
        if (SqlTypeName.EXACT_TYPES.contains(literal.getTypeName())) {
          final BigDecimal offset = (BigDecimal) literal.getValue4();

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
