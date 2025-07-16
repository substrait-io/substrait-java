package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.relation.Rel;
import java.util.Arrays;
import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.Test;

public class SchemaCollectorTest extends PlanTestBase {

  SubstraitBuilder b = substraitBuilder;
  SchemaCollector schemaCollector = new SchemaCollector(typeFactory, TypeConverter.DEFAULT);

  void hasTable(CalciteSchema schema, String tableName, String tableSchema) {
    CalciteSchema.TableEntry table = schema.getTable(tableName, false);
    assertNotNull(table);
    assertEquals(tableSchema, table.getTable().getRowType(typeFactory).getFullTypeString());
  }

  @Test
  void canCollectTables() {
    Rel rel =
        b.cross(
            b.namedScan(
                Arrays.asList("table1"),
                Arrays.asList("col1", "col2", "col3"),
                Arrays.asList(N.I64, R.FP64, N.STRING)),
            b.namedScan(
                Arrays.asList("table2"),
                Arrays.asList("col4", "col5"),
                Arrays.asList(N.BOOLEAN, N.I32)));
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
        b.cross(
            b.cross(
                b.namedScan(
                    Arrays.asList("schema1", "table1"),
                    Arrays.asList("col1", "col2", "col3"),
                    Arrays.asList(N.I64, N.FP64, N.STRING)),
                b.namedScan(
                    Arrays.asList("schema1", "table2"),
                    Arrays.asList("col4", "col5"),
                    Arrays.asList(N.BOOLEAN, N.I32))),
            b.namedScan(
                Arrays.asList("schema2", "table3"), Arrays.asList("col6"), Arrays.asList(N.I64)));
    CalciteSchema calciteSchema = schemaCollector.toSchema(rel);

    CalciteSchema schema1 = calciteSchema.getSubSchema("schema1", false);
    hasTable(schema1, "table1", "RecordType(BIGINT col1, DOUBLE col2, VARCHAR col3) NOT NULL");
    hasTable(schema1, "table2", "RecordType(BOOLEAN col4, INTEGER col5) NOT NULL");

    CalciteSchema schema2 = calciteSchema.getSubSchema("schema2", false);
    hasTable(schema2, "table3", "RecordType(BIGINT col6) NOT NULL");
  }

  @Test
  void canHandleMultipleSchemas() {
    Rel rel =
        b.cross(
            b.namedScan(
                Arrays.asList("level1", "level2a", "level3", "t1"),
                Arrays.asList("col1"),
                Arrays.asList(N.I64)),
            b.namedScan(
                Arrays.asList("level1", "level2b", "t2"),
                Arrays.asList("col2"),
                Arrays.asList(N.I32)));

    var rootSchema = schemaCollector.toSchema(rel);
    CalciteSchema level1 = rootSchema.getSubSchema("level1", false);

    CalciteSchema level2a = level1.getSubSchema("level2a", false);
    CalciteSchema level3 = level2a.getSubSchema("level3", false);
    hasTable(level3, "t1", "RecordType(BIGINT col1) NOT NULL");

    CalciteSchema level2b = level1.getSubSchema("level2b", false);
    hasTable(level2b, "t2", "RecordType(INTEGER col2) NOT NULL");
  }

  @Test
  void canHandleDuplicateNamedScans() {
    Rel table =
        b.namedScan(Arrays.asList("table"), Arrays.asList("col1"), Arrays.asList(N.BOOLEAN));
    Rel rel = b.cross(table, table);

    CalciteSchema calciteSchema = schemaCollector.toSchema(rel);
    hasTable(calciteSchema, "table", "RecordType(BOOLEAN col1) NOT NULL");
  }

  @Test
  void validatesSchemasForDuplicateNamedScans() {
    Rel rel =
        b.cross(
            b.namedScan(Arrays.asList("t"), Arrays.asList("col1"), Arrays.asList(N.BOOLEAN)),
            b.namedScan(Arrays.asList("t"), Arrays.asList("col1"), Arrays.asList(R.BOOLEAN)));

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> schemaCollector.toSchema(rel));
    assertEquals(
        "NamedScan for [t] is present multiple times with different schemas",
        exception.getMessage());
  }

  @Test
  void validatesSchemasForNestedDuplicateNamedScans() {
    Rel rel =
        b.cross(
            b.namedScan(Arrays.asList("s", "t"), Arrays.asList("col1"), Arrays.asList(N.BOOLEAN)),
            b.namedScan(Arrays.asList("s", "t"), Arrays.asList("col1"), Arrays.asList(R.BOOLEAN)));

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> schemaCollector.toSchema(rel));
    assertEquals(
        "NamedScan for [s, t] is present multiple times with different schemas",
        exception.getMessage());
  }
}
