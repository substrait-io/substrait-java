package io.substrait.isthmus.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

class SubstraitCreateStatementParserTest {
  @Test
  void testToCatalogWithSingleCreateTableWithTableNameOnly() throws SqlParseException {
    final CalciteCatalogReader catalogReader =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "create table src1 (intcol int, charcol varchar(10))");

    final CalciteSchema rootSchema = catalogReader.getRootSchema();

    assertEquals(1, rootSchema.getTableNames().size());
    assertNotNull(rootSchema.getTable("src1", false));
  }

  @Test
  void testToCatalogWithMultipleCreateTableWithTableNameOnly() throws SqlParseException {
    final CalciteCatalogReader catalogReader =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "create table src1 (intcol int, charcol varchar(10))",
            "create table src2 (intcol int, charcol varchar(10))");

    final CalciteSchema rootSchema = catalogReader.getRootSchema();

    assertEquals(2, rootSchema.getTableNames().size());
    assertNotNull(rootSchema.getTable("src1", false));
    assertNotNull(rootSchema.getTable("src2", false));
  }

  @Test
  void testToCatalogWithSingleCreateTableWithSchemaAndTableName() throws SqlParseException {
    final CalciteCatalogReader catalogReader =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "create table schema1.src1 (intcol int, charcol varchar(10))");

    final CalciteSchema rootSchema = catalogReader.getRootSchema();

    assertEquals(0, rootSchema.getTableNames().size());
    assertEquals(1, rootSchema.getSubSchemaMap().size());
    assertNotNull(rootSchema.getSubSchema("schema1", false));
    assertNotNull(rootSchema.getSubSchema("schema1", false).getTable("src1", false));
  }

  @Test
  void testToCatalogWithMultipleCreateTableWithSameSchema() throws SqlParseException {
    final CalciteCatalogReader catalogReader =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "create table schema1.src1 (intcol int, charcol varchar(10))",
            "create table schema1.src2 (intcol int, charcol varchar(10))");

    final CalciteSchema rootSchema = catalogReader.getRootSchema();

    assertEquals(0, rootSchema.getTableNames().size());
    assertEquals(1, rootSchema.getSubSchemaMap().size());
    assertNotNull(rootSchema.getSubSchema("schema1", false));
    assertNotNull(rootSchema.getSubSchema("schema1", false).getTable("src1", false));
    assertNotNull(rootSchema.getSubSchema("schema1", false).getTable("src2", false));
  }

  @Test
  void testToCatalogWithMultipleCreateTableWithDifferentSchemas() throws SqlParseException {
    final CalciteCatalogReader catalogReader =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "create table schema1.src1 (intcol int, charcol varchar(10))",
            "create table schema2.src2 (intcol int, charcol varchar(10))");

    final CalciteSchema rootSchema = catalogReader.getRootSchema();

    assertEquals(0, rootSchema.getTableNames().size());
    assertEquals(2, rootSchema.getSubSchemaMap().size());
    assertNotNull(rootSchema.getSubSchema("schema1", false));
    assertNotNull(rootSchema.getSubSchema("schema1", false).getTable("src1", false));
    assertNotNull(rootSchema.getSubSchema("schema2", false));
    assertNotNull(rootSchema.getSubSchema("schema2", false).getTable("src2", false));
  }

  @Test
  void testToCatalogWithSingleCreateTableWithCatalogAndSchemaAndTableName()
      throws SqlParseException {
    final CalciteCatalogReader catalogReader =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "create table catalog1.schema1.src1 (intcol int, charcol varchar(10))");

    final CalciteSchema rootSchema = catalogReader.getRootSchema();

    assertEquals(0, rootSchema.getTableNames().size());
    assertEquals(1, rootSchema.getSubSchemaMap().size());
    assertNotNull(rootSchema.getSubSchema("catalog1", false));
    assertNotNull(rootSchema.getSubSchema("catalog1", false).getSubSchema("schema1", false));
    assertNotNull(
        rootSchema
            .getSubSchema("catalog1", false)
            .getSubSchema("schema1", false)
            .getTable("src1", false));
  }

  @Test
  void testToCatalogWithMultipleCreateTableForSameTableThrowsException() throws SqlParseException {
    assertThrows(
        SqlParseException.class,
        () ->
            SubstraitCreateStatementParser.processCreateStatementsToCatalog(
                "create table src1 (intcol int, charcol varchar(10))",
                "create table src1 (intcol int, charcol varchar(20))"));
  }
}
