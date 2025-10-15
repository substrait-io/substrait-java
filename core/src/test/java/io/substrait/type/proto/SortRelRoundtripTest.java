package io.substrait.type.proto;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.relation.Rel;
import io.substrait.relation.Sort;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

public class SortRelRoundtripTest extends TestBase {

  final Rel baseTable =
      b.namedScan(
          Collections.singletonList("test_table"),
          Arrays.asList("id", "amount", "name", "category", "timestamp"),
          Arrays.asList(R.I64, R.FP64, R.STRING, R.STRING, R.TIMESTAMP));

  @Test
  void simpleSortAscending() {
    // Sort by id ascending, nulls first
    Expression.SortField sortField =
        Expression.SortField.builder()
            .expr(b.fieldReference(baseTable, 0))
            .direction(Expression.SortDirection.ASC_NULLS_FIRST)
            .build();

    Rel sort = Sort.builder().input(baseTable).addSortFields(sortField).build();

    verifyRoundTrip(sort);
  }

  @Test
  void sortAscendingNullsLast() {
    // Sort by name ascending, nulls last
    Expression.SortField sortField =
        Expression.SortField.builder()
            .expr(b.fieldReference(baseTable, 2))
            .direction(Expression.SortDirection.ASC_NULLS_LAST)
            .build();

    Rel sort = Sort.builder().input(baseTable).addSortFields(sortField).build();

    verifyRoundTrip(sort);
  }

  @Test
  void sortDescendingNullsFirst() {
    // Sort by amount descending, nulls first
    Expression.SortField sortField =
        Expression.SortField.builder()
            .expr(b.fieldReference(baseTable, 1))
            .direction(Expression.SortDirection.DESC_NULLS_FIRST)
            .build();

    Rel sort = Sort.builder().input(baseTable).addSortFields(sortField).build();

    verifyRoundTrip(sort);
  }

  @Test
  void sortDescendingNullsLast() {
    // Sort by timestamp descending, nulls last
    Expression.SortField sortField =
        Expression.SortField.builder()
            .expr(b.fieldReference(baseTable, 4))
            .direction(Expression.SortDirection.DESC_NULLS_LAST)
            .build();

    Rel sort = Sort.builder().input(baseTable).addSortFields(sortField).build();

    verifyRoundTrip(sort);
  }

  @Test
  void sortClustered() {
    // Sort with clustered direction (no specific order guarantee)
    Expression.SortField sortField =
        Expression.SortField.builder()
            .expr(b.fieldReference(baseTable, 3))
            .direction(Expression.SortDirection.CLUSTERED)
            .build();

    Rel sort = Sort.builder().input(baseTable).addSortFields(sortField).build();

    verifyRoundTrip(sort);
  }

  @Test
  void multipleSortFields() {
    // Sort by category (asc), then amount (desc)
    Expression.SortField sortField1 =
        Expression.SortField.builder()
            .expr(b.fieldReference(baseTable, 3))
            .direction(Expression.SortDirection.ASC_NULLS_FIRST)
            .build();

    Expression.SortField sortField2 =
        Expression.SortField.builder()
            .expr(b.fieldReference(baseTable, 1))
            .direction(Expression.SortDirection.DESC_NULLS_LAST)
            .build();

    Rel sort = Sort.builder().input(baseTable).addSortFields(sortField1, sortField2).build();

    verifyRoundTrip(sort);
  }

  @Test
  void sortByThreeFields() {
    // Sort by category, name, and id
    Expression.SortField sortField1 =
        Expression.SortField.builder()
            .expr(b.fieldReference(baseTable, 3))
            .direction(Expression.SortDirection.ASC_NULLS_LAST)
            .build();

    Expression.SortField sortField2 =
        Expression.SortField.builder()
            .expr(b.fieldReference(baseTable, 2))
            .direction(Expression.SortDirection.ASC_NULLS_LAST)
            .build();

    Expression.SortField sortField3 =
        Expression.SortField.builder()
            .expr(b.fieldReference(baseTable, 0))
            .direction(Expression.SortDirection.ASC_NULLS_FIRST)
            .build();

    Rel sort =
        Sort.builder().input(baseTable).addSortFields(sortField1, sortField2, sortField3).build();

    verifyRoundTrip(sort);
  }

  @Test
  void sortByComputedExpression() {
    // Sort by computed expression: amount * 2
    Expression computedExpr = b.multiply(b.fieldReference(baseTable, 1), b.fp64(2.0));

    Expression.SortField sortField =
        Expression.SortField.builder()
            .expr(computedExpr)
            .direction(Expression.SortDirection.DESC_NULLS_LAST)
            .build();

    Rel sort = Sort.builder().input(baseTable).addSortFields(sortField).build();

    verifyRoundTrip(sort);
  }

  @Test
  void sortByStringField() {
    // Sort by string field directly
    Expression.SortField sortField =
        Expression.SortField.builder()
            .expr(b.fieldReference(baseTable, 2))
            .direction(Expression.SortDirection.ASC_NULLS_LAST)
            .build();

    Rel sort = Sort.builder().input(baseTable).addSortFields(sortField).build();

    verifyRoundTrip(sort);
  }

  @Test
  void sortWithMixedNullHandling() {
    // Sort with different null handling for different fields
    Expression.SortField sortField1 =
        Expression.SortField.builder()
            .expr(b.fieldReference(baseTable, 3))
            .direction(Expression.SortDirection.ASC_NULLS_FIRST)
            .build();

    Expression.SortField sortField2 =
        Expression.SortField.builder()
            .expr(b.fieldReference(baseTable, 1))
            .direction(Expression.SortDirection.DESC_NULLS_FIRST)
            .build();

    Expression.SortField sortField3 =
        Expression.SortField.builder()
            .expr(b.fieldReference(baseTable, 2))
            .direction(Expression.SortDirection.ASC_NULLS_LAST)
            .build();

    Rel sort =
        Sort.builder().input(baseTable).addSortFields(sortField1, sortField2, sortField3).build();

    verifyRoundTrip(sort);
  }

  @Test
  void sortAllDirections() {
    // Test all sort directions in single sort operation
    Rel sort =
        Sort.builder()
            .input(baseTable)
            .addSortFields(
                Expression.SortField.builder()
                    .expr(b.fieldReference(baseTable, 0))
                    .direction(Expression.SortDirection.ASC_NULLS_FIRST)
                    .build(),
                Expression.SortField.builder()
                    .expr(b.fieldReference(baseTable, 1))
                    .direction(Expression.SortDirection.ASC_NULLS_LAST)
                    .build(),
                Expression.SortField.builder()
                    .expr(b.fieldReference(baseTable, 2))
                    .direction(Expression.SortDirection.DESC_NULLS_FIRST)
                    .build(),
                Expression.SortField.builder()
                    .expr(b.fieldReference(baseTable, 3))
                    .direction(Expression.SortDirection.DESC_NULLS_LAST)
                    .build(),
                Expression.SortField.builder()
                    .expr(b.fieldReference(baseTable, 4))
                    .direction(Expression.SortDirection.CLUSTERED)
                    .build())
            .build();

    verifyRoundTrip(sort);
  }

  @Test
  void nestedSort() {
    // Sort on top of another sort
    Expression.SortField firstSort =
        Expression.SortField.builder()
            .expr(b.fieldReference(baseTable, 3))
            .direction(Expression.SortDirection.ASC_NULLS_FIRST)
            .build();

    Rel firstSortRel = Sort.builder().input(baseTable).addSortFields(firstSort).build();

    Expression.SortField secondSort =
        Expression.SortField.builder()
            .expr(b.fieldReference(firstSortRel, 0))
            .direction(Expression.SortDirection.DESC_NULLS_LAST)
            .build();

    Rel secondSortRel = Sort.builder().input(firstSortRel).addSortFields(secondSort).build();

    verifyRoundTrip(secondSortRel);
  }
}
