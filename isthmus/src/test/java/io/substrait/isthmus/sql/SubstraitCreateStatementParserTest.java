package io.substrait.isthmus.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
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

    assertEquals(Set.of("SRC1"), rootSchema.getTableNames());
  }

  @Test
  void testToCatalogWithMultipleCreateTableWithTableNameOnly() throws SqlParseException {
    final CalciteCatalogReader catalogReader =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "create table src1 (intcol int, charcol varchar(10))",
            "create table src2 (intcol int, charcol varchar(10))");

    final CalciteSchema rootSchema = catalogReader.getRootSchema();

    assertEquals(Set.of("SRC1", "SRC2"), rootSchema.getTableNames());
  }

  @Test
  void testToCatalogWithSingleCreateTableWithSchemaAndTableName() throws SqlParseException {
    final CalciteCatalogReader catalogReader =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "create table schema1.src1 (intcol int, charcol varchar(10))");

    final CalciteSchema rootSchema = catalogReader.getRootSchema();

    assertTrue(rootSchema.getTableNames().isEmpty());
    assertEquals(Set.of("SCHEMA1"), rootSchema.getSubSchemaMap().keySet());
    assertEquals(Set.of("SRC1"), rootSchema.getSubSchema("schema1", false).getTableNames());
  }

  @Test
  void testToCatalogWithMultipleCreateTableWithSameSchema() throws SqlParseException {
    final CalciteCatalogReader catalogReader =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "create table schema1.src1 (intcol int, charcol varchar(10))",
            "create table schema1.src2 (intcol int, charcol varchar(10))");

    final CalciteSchema rootSchema = catalogReader.getRootSchema();

    assertTrue(rootSchema.getTableNames().isEmpty());
    assertEquals(Set.of("SCHEMA1"), rootSchema.getSubSchemaMap().keySet());
    assertEquals(Set.of("SRC1", "SRC2"), rootSchema.getSubSchema("schema1", false).getTableNames());
  }

  @Test
  void testToCatalogWithMultipleCreateTableWithDifferentSchemas() throws SqlParseException {
    final CalciteCatalogReader catalogReader =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "create table schema1.src1 (intcol int, charcol varchar(10))",
            "create table schema2.src2 (intcol int, charcol varchar(10))");

    final CalciteSchema rootSchema = catalogReader.getRootSchema();

    assertTrue(rootSchema.getTableNames().isEmpty());
    assertEquals(Set.of("SCHEMA1", "SCHEMA2"), rootSchema.getSubSchemaMap().keySet());
    assertEquals(Set.of("SRC1"), rootSchema.getSubSchema("schema1", false).getTableNames());
    assertEquals(Set.of("SRC2"), rootSchema.getSubSchema("schema2", false).getTableNames());
  }

  @Test
  void testToCatalogWithSingleCreateTableWithCatalogAndSchemaAndTableName()
      throws SqlParseException {
    final CalciteCatalogReader catalogReader =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "create table catalog1.schema1.src1 (intcol int, charcol varchar(10))");

    final CalciteSchema rootSchema = catalogReader.getRootSchema();

    assertTrue(rootSchema.getTableNames().isEmpty());
    assertEquals(Set.of("CATALOG1"), rootSchema.getSubSchemaMap().keySet());
    assertEquals(
        Set.of("SCHEMA1"), rootSchema.getSubSchema("catalog1", false).getSubSchemaMap().keySet());
    assertEquals(
        Set.of("SRC1"),
        rootSchema.getSubSchema("catalog1", false).getSubSchema("schema1", false).getTableNames());
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
