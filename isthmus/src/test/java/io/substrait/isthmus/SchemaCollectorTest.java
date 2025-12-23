package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableFieldReference;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.relation.AbstractWriteRel;
import io.substrait.relation.NamedUpdate;
import io.substrait.relation.Rel;
import io.substrait.type.TypeCreator;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.Test;

class SchemaCollectorTest extends PlanTestBase {

  SchemaCollector schemaCollector = new SchemaCollector(typeFactory, TypeConverter.DEFAULT);

  void hasTable(CalciteSchema schema, String tableName, String tableSchema) {
    CalciteSchema.TableEntry table = schema.getTable(tableName, false);
    assertNotNull(table);
    assertEquals(tableSchema, table.getTable().getRowType(typeFactory).getFullTypeString());
  }

  @Test
  void canCollectTables() {
    Rel rel =
        sb.cross(
            sb.namedScan(
                List.of("table1"),
                List.of("col1", "col2", "col3"),
                List.of(N.I64, R.FP64, N.STRING)),
            sb.namedScan(List.of("table2"), List.of("col4", "col5"), List.of(N.BOOLEAN, N.I32)));
    CalciteSchema calciteSchema = schemaCollector.toSchema(rel);

    hasTable(
        calciteSchema,
        "table1",
        "RecordType(BIGINT col1, DOUBLE NOT NULL col2, VARCHAR col3) NOT NULL");
    hasTable(calciteSchema, "table2", "RecordType(BOOLEAN col4, INTEGER col5) NOT NULL");
  }

  @Test
  void canCollectTablesInSchemas() {
    Rel rel =
        sb.namedWrite(
            List.of("schema3", "table4"),
            List.of("col1", "col2", "col3", "col4", "col5", "col6"),
            AbstractWriteRel.WriteOp.UPDATE,
            AbstractWriteRel.CreateMode.REPLACE_IF_EXISTS,
            AbstractWriteRel.OutputMode.MODIFIED_RECORDS,
            sb.cross(
                sb.cross(
                    sb.namedScan(
                        List.of("schema1", "table1"),
                        List.of("col1", "col2", "col3"),
                        List.of(N.I64, N.FP64, N.STRING)),
                    sb.namedScan(
                        List.of("schema1", "table2"),
                        List.of("col4", "col5"),
                        List.of(N.BOOLEAN, N.I32))),
                sb.namedScan(List.of("schema2", "table3"), List.of("col6"), List.of(N.I64))));
    CalciteSchema calciteSchema = schemaCollector.toSchema(rel);

    CalciteSchema schema1 = calciteSchema.getSubSchema("schema1", false);
    hasTable(schema1, "table1", "RecordType(BIGINT col1, DOUBLE col2, VARCHAR col3) NOT NULL");
    hasTable(schema1, "table2", "RecordType(BOOLEAN col4, INTEGER col5) NOT NULL");

    CalciteSchema schema2 = calciteSchema.getSubSchema("schema2", false);
    hasTable(schema2, "table3", "RecordType(BIGINT col6) NOT NULL");

    CalciteSchema schema3 = calciteSchema.getSubSchema("schema3", false);
    hasTable(
        schema3,
        "table4",
        "RecordType(BIGINT col1, DOUBLE col2, VARCHAR col3, BOOLEAN col4, INTEGER col5, BIGINT col6) NOT NULL");
  }

  private static Expression.ScalarFunctionInvocation fnAdd(int value) {
    return DefaultExtensionCatalog.DEFAULT_COLLECTION.scalarFunctions().stream()
        .filter(s -> s.name().equalsIgnoreCase("add"))
        .findFirst()
        .map(
            declaration ->
                ExpressionCreator.scalarFunction(
                    declaration,
                    TypeCreator.REQUIRED.BOOLEAN,
                    ImmutableFieldReference.builder()
                        .addSegments(FieldReference.StructField.of(0))
                        .type(TypeCreator.REQUIRED.I64)
                        .build(),
                    ExpressionCreator.i32(false, value)))
        .get();
  }

  @Test
  void testUpdate() {

    List<NamedUpdate.TransformExpression> transformations =
        Arrays.asList(
            NamedUpdate.TransformExpression.builder()
                .columnTarget(0)
                .transformation(fnAdd(1))
                .build());
    Expression condition = ExpressionCreator.bool(false, true);

    Rel rel =
        sb.namedWrite(
            List.of("schema1", "table2"),
            List.of("col1"),
            AbstractWriteRel.WriteOp.INSERT,
            AbstractWriteRel.CreateMode.APPEND_IF_EXISTS,
            AbstractWriteRel.OutputMode.NO_OUTPUT,
            sb.namedUpdate(
                List.of("schema1", "table1"), List.of("col1"), transformations, condition, true));

    CalciteSchema calciteSchema = schemaCollector.toSchema(rel);
    CalciteSchema schema1 = calciteSchema.getSubSchema("schema1", false);
    hasTable(schema1, "table1", "RecordType(BOOLEAN col1)");

    hasTable(schema1, "table2", "RecordType(BOOLEAN col1)");
  }

  @Test
  void canHandleMultipleSchemas() {
    Rel rel =
        sb.cross(
            sb.namedScan(
                List.of("level1", "level2a", "level3", "t1"), List.of("col1"), List.of(N.I64)),
            sb.namedScan(List.of("level1", "level2b", "t2"), List.of("col2"), List.of(N.I32)));

    CalciteSchema rootSchema = schemaCollector.toSchema(rel);
    CalciteSchema level1 = rootSchema.getSubSchema("level1", false);

    CalciteSchema level2a = level1.getSubSchema("level2a", false);
    CalciteSchema level3 = level2a.getSubSchema("level3", false);
    hasTable(level3, "t1", "RecordType(BIGINT col1) NOT NULL");

    CalciteSchema level2b = level1.getSubSchema("level2b", false);
    hasTable(level2b, "t2", "RecordType(INTEGER col2) NOT NULL");
  }

  @Test
  void canHandleDuplicateNamedScans() {
    Rel table = sb.namedScan(List.of("table"), List.of("col1"), List.of(N.BOOLEAN));
    Rel rel = sb.cross(table, table);

    CalciteSchema calciteSchema = schemaCollector.toSchema(rel);
    hasTable(calciteSchema, "table", "RecordType(BOOLEAN col1) NOT NULL");
  }

  @Test
  void validatesSchemasForDuplicateNamedScans() {
    Rel rel =
        sb.cross(
            sb.namedScan(List.of("t"), List.of("col1"), List.of(N.BOOLEAN)),
            sb.namedScan(List.of("t"), List.of("col1"), List.of(R.BOOLEAN)));

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> schemaCollector.toSchema(rel));
    assertEquals(
        "NamedScan for [t] is present multiple times with different schemas",
        exception.getMessage());
  }

  @Test
  void validatesSchemasForNestedDuplicateNamedScans() {
    Rel rel =
        sb.cross(
            sb.namedScan(List.of("s", "t"), List.of("col1"), List.of(N.BOOLEAN)),
            sb.namedScan(List.of("s", "t"), List.of("col1"), List.of(R.BOOLEAN)));

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> schemaCollector.toSchema(rel));
    assertEquals(
        "NamedScan for [s, t] is present multiple times with different schemas",
        exception.getMessage());
  }
}
