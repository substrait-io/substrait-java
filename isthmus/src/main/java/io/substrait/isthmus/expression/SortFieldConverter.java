package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rex.RexFieldCollation;

/**
 * Utility for converting Calcite {@link RexFieldCollation} objects into Substrait {@link
 * Expression.SortField} representations.
 *
 * <p>Handles sort direction and null ordering.
 */
public class SortFieldConverter {

  /**
   * Converts a Calcite {@link RexFieldCollation} to a Substrait {@link Expression.SortField}.
   *
   * @param rexFieldCollation The Calcite field collation to convert.
   * @param rexExpressionConverter Converter for translating the field expression.
   * @return A Substrait {@link Expression.SortField} with the appropriate direction and expression.
   * @throws IllegalArgumentException if the collation direction is unsupported.
   */
  public static Expression.SortField toSortField(
      RexFieldCollation rexFieldCollation, RexExpressionConverter rexExpressionConverter) {
    Expression expr = rexFieldCollation.left.accept(rexExpressionConverter);
    Expression.SortDirection direction = asSortDirection(rexFieldCollation);

    return Expression.SortField.builder().expr(expr).direction(direction).build();
  }

  /**
   * Determines the Substrait {@link Expression.SortDirection} based on Calcite collation details.
   *
   * @param collation The Calcite {@link RexFieldCollation}.
   * @return The corresponding Substrait sort direction.
   * @throws IllegalArgumentException if the direction is not ASCENDING or DESCENDING.
   */
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
