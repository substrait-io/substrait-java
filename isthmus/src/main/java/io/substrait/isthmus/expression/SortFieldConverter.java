package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rex.RexFieldCollation;

public class SortFieldConverter {

  /** Converts a {@link RexFieldCollation} to a {@link Expression.SortField}. */
  public static Expression.SortField toSortField(
      RexFieldCollation rexFieldCollation, RexExpressionConverter rexExpressionConverter) {
    var expr = rexFieldCollation.left.accept(rexExpressionConverter);
    var rexDirection = rexFieldCollation.getDirection();
    Expression.SortDirection direction =
        switch (rexDirection) {
          case ASCENDING -> rexFieldCollation.getNullDirection()
                  == RelFieldCollation.NullDirection.LAST
              ? Expression.SortDirection.ASC_NULLS_LAST
              : Expression.SortDirection.ASC_NULLS_FIRST;
          case DESCENDING -> rexFieldCollation.getNullDirection()
                  == RelFieldCollation.NullDirection.LAST
              ? Expression.SortDirection.DESC_NULLS_LAST
              : Expression.SortDirection.DESC_NULLS_FIRST;
          default -> throw new IllegalArgumentException(
              String.format(
                  "Unexpected RelFieldCollation.Direction:%s enum at the RexFieldCollation!",
                  rexDirection));
        };

    return Expression.SortField.builder().expr(expr).direction(direction).build();
  }
}
