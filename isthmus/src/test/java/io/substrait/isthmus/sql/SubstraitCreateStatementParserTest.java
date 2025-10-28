package io.substrait.isthmus.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

class SubstraitCreateStatementParserTest {
  @Test
  void testProcessCreateStatementsToCatalogWithSingleCreateTableWithTableNameOnly()
      throws SqlParseException {
    CalciteCatalogReader catalogReader =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "create table src1 (intcol int, charcol varchar(10))");

    assertEquals(catalogReader.getRootSchema().getTableNames().size(), 1);
    assertNotNull(catalogReader.getRootSchema().getTable("src1", false));
  }

  @Test
  void testProcessCreateStatementsToCatalogWithMultipleCreateTableWithTableNameOnly()
      throws SqlParseException {
    CalciteCatalogReader catalogReader =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "create table src1 (intcol int, charcol varchar(10))",
            "create table src2 (intcol int, charcol varchar(10))");

    assertEquals(catalogReader.getRootSchema().getTableNames().size(), 2);
    assertNotNull(catalogReader.getRootSchema().getTable("src1", false));
    assertNotNull(catalogReader.getRootSchema().getTable("src2", false));
  }

  @Test
  void testProcessCreateStatementsToCatalogWithSingleCreateTableWithSchemaAndTableName()
      throws SqlParseException {
    CalciteCatalogReader catalogReader =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "create table schema1.src1 (intcol int, charcol varchar(10))");

    assertEquals(catalogReader.getRootSchema().getTableNames().size(), 0);
    assertEquals(catalogReader.getRootSchema().getSubSchemaMap().size(), 1);
    assertNotNull(catalogReader.getRootSchema().getSubSchema("schema1", false));
    assertNotNull(
        catalogReader.getRootSchema().getSubSchema("schema1", false).getTable("src1", false));
  }

  @Test
  void testProcessCreateStatementsToCatalogWithMultipleCreateTableWithSameSchema()
      throws SqlParseException {
    CalciteCatalogReader catalogReader =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "create table schema1.src1 (intcol int, charcol varchar(10))",
            "create table schema1.src2 (intcol int, charcol varchar(10))");

    assertEquals(catalogReader.getRootSchema().getTableNames().size(), 0);
    assertEquals(catalogReader.getRootSchema().getSubSchemaMap().size(), 1);
    assertNotNull(catalogReader.getRootSchema().getSubSchema("schema1", false));
    assertNotNull(
        catalogReader.getRootSchema().getSubSchema("schema1", false).getTable("src1", false));
    assertNotNull(
        catalogReader.getRootSchema().getSubSchema("schema1", false).getTable("src2", false));
  }

  @Test
  void testProcessCreateStatementsToCatalogWithMultipleCreateTableWithDifferentSchemas()
      throws SqlParseException {
    CalciteCatalogReader catalogReader =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "create table schema1.src1 (intcol int, charcol varchar(10))",
            "create table schema2.src2 (intcol int, charcol varchar(10))");

    assertEquals(catalogReader.getRootSchema().getTableNames().size(), 0);
    assertEquals(catalogReader.getRootSchema().getSubSchemaMap().size(), 2);
    assertNotNull(catalogReader.getRootSchema().getSubSchema("schema1", false));
    assertNotNull(
        catalogReader.getRootSchema().getSubSchema("schema1", false).getTable("src1", false));
    assertNotNull(catalogReader.getRootSchema().getSubSchema("schema2", false));
    assertNotNull(
        catalogReader.getRootSchema().getSubSchema("schema2", false).getTable("src2", false));
  }

  @Test
  void testProcessCreateStatementsToCatalogWithSingleCreateTableWithCatalogAndSchemaAndTableName()
      throws SqlParseException {
    CalciteCatalogReader catalogReader =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "create table catalog1.schema1.src1 (intcol int, charcol varchar(10))");

    assertEquals(catalogReader.getRootSchema().getTableNames().size(), 0);
    assertEquals(catalogReader.getRootSchema().getSubSchemaMap().size(), 1);
    assertNotNull(catalogReader.getRootSchema().getSubSchema("catalog1", false));
    assertNotNull(
        catalogReader
            .getRootSchema()
            .getSubSchema("catalog1", false)
            .getSubSchema("schema1", false));
    assertNotNull(
        catalogReader
            .getRootSchema()
            .getSubSchema("catalog1", false)
            .getSubSchema("schema1", false)
            .getTable("src1", false));
  }

  @Test
  void testProcessCreateStatementsToCatalogWithMultipleCreateTableForSameTableThrowsException()
      throws SqlParseException {
    assertThrows(
        SqlParseException.class,
        () ->
            SubstraitCreateStatementParser.processCreateStatementsToCatalog(
                "create table src1 (intcol int, charcol varchar(10))",
                "create table src1 (intcol int, charcol varchar(20))"));
  }
}
