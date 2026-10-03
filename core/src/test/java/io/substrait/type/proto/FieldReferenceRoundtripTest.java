package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.relation.Filter;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests field reference roundtrip behavior through relations. Field references are tested as part
 * of the relation context since they require schema information for proper deserialization.
 */
class FieldReferenceRoundtripTest extends TestBase {

  final Rel baseTable =
      sb.namedScan(
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
    Rel projection =
        Project.builder().input(baseTable).addExpressions(sb.fieldReference(baseTable, 0)).build();

    verifyRoundTrip(projection);
  }

  @Test
  void multipleFieldReferences() {
    // Test multiple field references in same projection
    Rel projection =
        Project.builder()
            .input(baseTable)
            .addExpressions(
                sb.fieldReference(baseTable, 0),
                sb.fieldReference(baseTable, 1),
                sb.fieldReference(baseTable, 2))
            .build();

    verifyRoundTrip(projection);
  }

  @Test
  void fieldReferenceInFilter() {
    // Test field reference in filter condition
    Expression condition =
        sb.equal(sb.fieldReference(baseTable, 0), sb.fieldReference(baseTable, 0));

    Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void fieldReferenceInComplexExpression() {
    // Test field reference as part of arithmetic expression
    Expression add = sb.add(sb.fieldReference(baseTable, 0), sb.fieldReference(baseTable, 0));

    Rel projection = Project.builder().input(baseTable).addExpressions(add).build();

    verifyRoundTrip(projection);
  }

  @Test
  void fieldReferenceInNestedProjection() {
    // Test field reference through nested projections
    Rel firstProjection =
        Project.builder()
            .input(baseTable)
            .addExpressions(sb.fieldReference(baseTable, 0), sb.fieldReference(baseTable, 2))
            .build();

    Rel secondProjection =
        Project.builder()
            .input(firstProjection)
            .addExpressions(sb.fieldReference(firstProjection, 1))
            .build();

    verifyRoundTrip(secondProjection);
  }

  @Test
  void fieldReferenceAllFields() {
    // Test referencing all fields
    Rel projection =
        Project.builder()
            .input(baseTable)
            .addExpressions(
                sb.fieldReference(baseTable, 0),
                sb.fieldReference(baseTable, 1),
                sb.fieldReference(baseTable, 2),
                sb.fieldReference(baseTable, 3))
            .build();

    verifyRoundTrip(projection);
  }

  @Test
  void fieldReferenceWithBooleanLogic() {
    // Test field references in boolean expressions
    Expression condition =
        sb.and(
            sb.equal(sb.fieldReference(baseTable, 0), sb.fieldReference(baseTable, 0)),
            sb.equal(sb.fieldReference(baseTable, 2), sb.str("test")));

    Rel filter = Filter.builder().input(baseTable).condition(condition).build();

    verifyRoundTrip(filter);
  }

  @Test
  void fieldReferenceInMultipleArithmetic() {
    // Test multiple field references in arithmetic
    Expression add = sb.add(sb.fieldReference(baseTable, 1), sb.fieldReference(baseTable, 1));
    Expression multiply = sb.multiply(add, sb.fieldReference(baseTable, 1));

    Rel projection = Project.builder().input(baseTable).addExpressions(multiply).build();

    verifyRoundTrip(projection);
  }

  @Test
  void fieldReferenceReordering() {
    // Test field reordering through projection (accessing fields out of order)
    Rel projection =
        Project.builder()
            .input(baseTable)
            .addExpressions(
                sb.fieldReference(baseTable, 3),
                sb.fieldReference(baseTable, 0),
                sb.fieldReference(baseTable, 2))
            .build();

    verifyRoundTrip(projection);
  }

  @Test
  void sameFieldReferencedMultipleTimes() {
    // Test same field referenced multiple times
    Rel projection =
        Project.builder()
            .input(baseTable)
            .addExpressions(
                sb.fieldReference(baseTable, 0),
                sb.fieldReference(baseTable, 0),
                sb.fieldReference(baseTable, 0))
            .build();

    verifyRoundTrip(projection);
  }

  @Test
  void listSelectionsRemainNullableThroughNestedFields() {
    Rel input =
        sb.namedScan(
            List.of("arrays"),
            List.of("lists", "rows", "maps"),
            List.of(
                R.list(R.list(R.I32)), R.list(R.struct(R.I32)), R.list(R.map(R.STRING, R.I32))));
    for (int offset : new int[] {0, -1, Integer.MAX_VALUE}) {
      List<Expression> selections =
          List.of(
              FieldReference.newInputRelReference(0, input)
                  .dereferenceList(offset)
                  .dereferenceList(0),
              FieldReference.newInputRelReference(1, input)
                  .dereferenceList(offset)
                  .dereferenceStruct(0),
              FieldReference.newInputRelReference(2, input)
                  .dereferenceList(offset)
                  .dereferenceMap(ExpressionCreator.string(false, "key")));
      selections.forEach(selection -> assertEquals(N.I32, selection.getType()));
      verifyRoundTrip(Project.builder().input(input).expressions(selections).build());
    }
  }

  @Test
  void listSelectionOnExpressionRemainsNullable() {
    Expression input = ExpressionCreator.list(false, ExpressionCreator.i32(false, 1));
    for (int offset : new int[] {0, Integer.MAX_VALUE}) {
      FieldReference selection = FieldReference.newListReference(offset, input);
      assertEquals(N.I32, selection.getType());
      verifyRoundTrip(selection);
    }
  }
}
