package io.substrait.type.proto;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

public class ProjectRelRoundtripTest extends TestBase {

  final Rel baseTable =
      b.namedScan(
          Collections.singletonList("test_table"),
          Arrays.asList("col_a", "col_b", "col_c", "col_d"),
          Arrays.asList(R.I64, R.FP64, R.STRING, R.I32));

  final Rel emptyTable = b.emptyScan();

  @Test
  void simpleProjection() {
    // Project single field
    Rel projection =
        Project.builder().input(baseTable).addExpressions(b.fieldReference(baseTable, 0)).build();

    verifyRoundTrip(projection);
  }

  @Test
  void multipleFieldProjection() {
    // Project multiple fields
    Rel projection =
        Project.builder()
            .input(baseTable)
            .addExpressions(
                b.fieldReference(baseTable, 0),
                b.fieldReference(baseTable, 2),
                b.fieldReference(baseTable, 1))
            .build();

    verifyRoundTrip(projection);
  }

  @Test
  void projectionWithComputedExpression() {
    // Project with computed expression: col_a + 3 (both I64)
    Expression addExpr = b.add(b.fieldReference(baseTable, 0), b.fieldReference(baseTable, 0));

    Rel projection = Project.builder().input(baseTable).addExpressions(addExpr).build();

    verifyRoundTrip(projection);
  }

  @Test
  void projectionWithMultipleComputedExpressions() {
    // Project with multiple computed expressions
    Expression add = b.add(b.fieldReference(baseTable, 0), b.fieldReference(baseTable, 0));
    Expression multiply =
        b.multiply(b.fieldReference(baseTable, 1), b.fieldReference(baseTable, 1));

    Rel projection =
        Project.builder()
            .input(baseTable)
            .addExpressions(
                b.fieldReference(baseTable, 2), // original field
                add, // computed col_a + 100
                multiply) // computed col_b * 2.0
            .build();

    verifyRoundTrip(projection);
  }

  @Test
  void projectionWithLiterals() {
    // Project with literal values
    Rel projection =
        Project.builder()
            .input(baseTable)
            .addExpressions(b.fieldReference(baseTable, 0), b.i32(100), b.str("constant_string"))
            .build();

    verifyRoundTrip(projection);
  }

  @Test
  void projectionWithAllFields() {
    // Project all fields (identity projection)
    Rel projection =
        Project.builder()
            .input(baseTable)
            .addExpressions(
                b.fieldReference(baseTable, 0),
                b.fieldReference(baseTable, 1),
                b.fieldReference(baseTable, 2),
                b.fieldReference(baseTable, 3))
            .build();

    verifyRoundTrip(projection);
  }

  @Test
  void nestedProjection() {
    // Project on top of another projection
    Rel firstProjection =
        Project.builder()
            .input(baseTable)
            .addExpressions(b.fieldReference(baseTable, 0), b.fieldReference(baseTable, 2))
            .build();

    Rel secondProjection =
        Project.builder()
            .input(firstProjection)
            .addExpressions(b.fieldReference(firstProjection, 1))
            .build();

    verifyRoundTrip(secondProjection);
  }

  @Test
  void projectionWithComparison() {
    // Project with comparison expression: col_a = col_d
    Expression comparison = b.equal(b.fieldReference(baseTable, 0), b.fieldReference(baseTable, 3));

    Rel projection =
        Project.builder()
            .input(baseTable)
            .addExpressions(b.fieldReference(baseTable, 0), comparison)
            .build();

    verifyRoundTrip(projection);
  }

  @Test
  void projectionWithCast() {
    // Project with type cast: CAST(col_d AS BIGINT)
    Expression cast = b.cast(b.fieldReference(baseTable, 3), R.I64);

    Rel projection = Project.builder().input(baseTable).addExpressions(cast).build();

    verifyRoundTrip(projection);
  }

  @Test
  void emptyProjection() {
    // Project with no expressions (edge case - may produce empty output schema)
    Rel projection = Project.builder().input(baseTable).build();

    verifyRoundTrip(projection);
  }

  @Test
  void avoidProjectRemapOnEmptyInput() {
    Rel projection =
        Project.builder().input(emptyTable).addExpressions(b.add(b.i32(1), b.i32(2))).build();
    verifyRoundTrip(projection);
  }
}
