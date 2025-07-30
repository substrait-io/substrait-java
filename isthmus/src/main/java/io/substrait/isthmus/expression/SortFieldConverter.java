package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rex.RexFieldCollation;

public class SortFieldConverter {

  /** Converts a {@link RexFieldCollation} to a {@link Expression.SortField}. */
  public static Expression.SortField toSortField(
      RexFieldCollation rexFieldCollation, RexExpressionConverter rexExpressionConverter) {
    Expression expr = rexFieldCollation.left.accept(rexExpressionConverter);
    Expression.SortDirection direction = asSortDirection(rexFieldCollation);

    return Expression.SortField.builder().expr(expr).direction(direction).build();
  }

  private static Expression.SortDirection asSortDirection(RexFieldCollation collation) {
    RelFieldCollation.Direction direction = collation.getDirection();

    if (direction == Direction.ASCENDING) {
      return collation.getNullDirection() == RelFieldCollation.NullDirection.LAST
          ? Expression.SortDirection.ASC_NULLS_LAST
          : Expression.SortDirection.ASC_NULLS_FIRST;
    }
    if (direction == Direction.DESCENDING) {
      return collation.getNullDirection() == RelFieldCollation.NullDirection.LAST
          ? Expression.SortDirection.DESC_NULLS_LAST
          : Expression.SortDirection.DESC_NULLS_FIRST;
    }

    throw new IllegalArgumentException(
        String.format(
            "Unexpected RelFieldCollation.Direction:%s enum at the RexFieldCollation!", direction));
  }
}
