package io.substrait.isthmus;

import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

public class DdlToSubstraitConversionTest {
  @Test
  void testConversion() throws SqlParseException {
    final Prepare.CatalogReader catalogReader =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "create table src1 (intcol int, charcol varchar(10))");

    final SqlToSubstrait converter = new SqlToSubstrait();
    converter.convert(
        "create table dst1 as select * from src1",
        catalogReader,
        SqlDialect.DatabaseProduct.CALCITE.getDialect());
  }
}
