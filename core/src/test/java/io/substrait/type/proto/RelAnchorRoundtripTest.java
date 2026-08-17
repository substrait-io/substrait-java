package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.relation.Filter;
import io.substrait.relation.NamedUpdate;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.physical.TopN;
import io.substrait.type.NamedStruct;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

/**
 * Round-trip tests for the {@link Rel#getRelAnchor()} field (Substrait v0.89.0 {@code
 * RelCommon.rel_anchor}), the plan-wide unique identifier used as the binding point for id-based
 * outer references.
 */
class RelAnchorRoundtripTest extends TestBase {

  final Rel baseTable =
      sb.namedScan(
          Collections.singletonList("test_table"),
          Arrays.asList("id", "name"),
          Arrays.asList(R.I64, R.STRING));

  @Test
  void relAnchorOnProject() {
    Rel projection =
        Project.builder()
            .input(baseTable)
            .relAnchor(7)
            .addExpressions(sb.fieldReference(baseTable, 0))
            .build();

    assertEquals(7, projection.getRelAnchor().orElseThrow(AssertionError::new));
    verifyRoundTrip(projection);
  }

  @Test
  void relAnchorOnFilter() {
    Rel filter =
        Filter.builder()
            .input(baseTable)
            .relAnchor(1)
            .condition(sb.equal(sb.fieldReference(baseTable, 0), sb.fieldReference(baseTable, 0)))
            .build();

    verifyRoundTrip(filter);
  }

  @Test
  void relAnchorOnTopN() {
    Rel topN =
        TopN.builder()
            .input(baseTable)
            .relAnchor(3)
            .addSortFields(
                Expression.SortField.builder()
                    .expr(sb.fieldReference(baseTable, 0))
                    .direction(Expression.SortDirection.ASC_NULLS_FIRST)
                    .build())
            .count(sb.i64(10))
            .build();

    verifyRoundTrip(topN);
  }

  @Test
  void relAnchorOnNamedUpdate() {
    Rel update =
        NamedUpdate.builder()
            .tableSchema(NamedStruct.of(Arrays.asList("id", "name"), R.struct(R.I64, R.STRING)))
            .names(Collections.singletonList("test_table"))
            .relAnchor(9)
            .condition(sb.bool(true))
            .build();

    verifyRoundTrip(update);
  }

  @Test
  void relAnchorAbsentByDefault() {
    Rel projection =
        Project.builder().input(baseTable).addExpressions(sb.fieldReference(baseTable, 0)).build();

    assertEquals(false, projection.getRelAnchor().isPresent());
    verifyRoundTrip(projection);
  }
}
