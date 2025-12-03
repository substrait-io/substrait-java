package io.substrait.type.proto;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.relation.Filter;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

/**
 * Tests field reference roundtrip behavior through relations. Field references are tested as part
 * of the relation context since they require schema information for proper deserialization.
 */
class FieldReferenceRoundtripTest extends TestBase {

  final Rel baseTable =
      b.namedScan(
          Collections.singletonList("test_table"),
          Arrays.asList("id", "amount", "name", "nested_struct"),
          Arrays.asList(
              R.I64,
              R.FP64,
              R.STRING,
              Type.Struct.builder().nullable(false).addFields(R.I32, R.STRING, R.BOOLEAN).build()));

  @Test
  void simpleStructFieldReference() {
    // Test simple root struct field reference via projection
    final Rel projection =
        Project.builder().input(baseTable).addExpressions(b.fieldReference(baseTable, 0)).build();

    verifyRoundTrip(projection);
  }

  @Test
  void multipleFieldReferences() {
    // Test multiple field references in same projection
    final Rel projection =
        Project.builder()
            .input(baseTable)
            .addExpressions(
                b.fieldReference(baseTable, 0),
                b.fieldReference(baseTable, 1),
                b.fieldReference(baseTable, 2))
            .build();

    verifyRoundTrip(projection);
  }

  @Test
  void fieldReferenceInFilter() {
    // Test field reference in filter condition
    final Expression condition =
        b.equal(b.fieldReference(baseTable, 0), b.fieldReference(baseTable, 0));

    final Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void fieldReferenceInComplexExpression() {
    // Test field reference as part of arithmetic expression
    final Expression add = b.add(b.fieldReference(baseTable, 0), b.fieldReference(baseTable, 0));

    final Rel projection = Project.builder().input(baseTable).addExpressions(add).build();

    verifyRoundTrip(projection);
  }

  @Test
  void fieldReferenceInNestedProjection() {
    // Test field reference through nested projections
    final Rel firstProjection =
        Project.builder()
            .input(baseTable)
            .addExpressions(b.fieldReference(baseTable, 0), b.fieldReference(baseTable, 2))
            .build();

    final Rel secondProjection =
        Project.builder()
            .input(firstProjection)
            .addExpressions(b.fieldReference(firstProjection, 1))
            .build();

    verifyRoundTrip(secondProjection);
  }

  @Test
  void fieldReferenceAllFields() {
    // Test referencing all fields
    final Rel projection =
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
  void fieldReferenceWithBooleanLogic() {
    // Test field references in boolean expressions
    final Expression condition =
        b.and(
            b.equal(b.fieldReference(baseTable, 0), b.fieldReference(baseTable, 0)),
            b.equal(b.fieldReference(baseTable, 2), b.str("test")));

    final Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void fieldReferenceInMultipleArithmetic() {
    // Test multiple field references in arithmetic
    final Expression add = b.add(b.fieldReference(baseTable, 1), b.fieldReference(baseTable, 1));
    final Expression multiply = b.multiply(add, b.fieldReference(baseTable, 1));

    final Rel projection = Project.builder().input(baseTable).addExpressions(multiply).build();

    verifyRoundTrip(projection);
  }

  @Test
  void fieldReferenceReordering() {
    // Test field reordering through projection (accessing fields out of order)
    final Rel projection =
        Project.builder()
            .input(baseTable)
            .addExpressions(
                b.fieldReference(baseTable, 3),
                b.fieldReference(baseTable, 0),
                b.fieldReference(baseTable, 2))
            .build();

    verifyRoundTrip(projection);
  }

  @Test
  void sameFieldReferencedMultipleTimes() {
    // Test same field referenced multiple times
    final Rel projection =
        Project.builder()
            .input(baseTable)
            .addExpressions(
                b.fieldReference(baseTable, 0),
                b.fieldReference(baseTable, 0),
                b.fieldReference(baseTable, 0))
            .build();

    verifyRoundTrip(projection);
  }
}
