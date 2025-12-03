package io.substrait.isthmus;

import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

class DdlRoundtripTest extends PlanTestBase {
  final Prepare.CatalogReader catalogReader =
      SubstraitCreateStatementParser.processCreateStatementsToCatalog(
          "create table src1 (intcol int, charcol varchar(10))",
          "create table src2 (intcol int, charcol varchar(10))");

  public DdlRoundtripTest() throws SqlParseException {
    super();
  }

  @Test
  void testCreateTable() throws Exception {
    final String sql = "create table dst1 as select * from src1";
    assertFullRoundTripWithIdentityProjectionWorkaround(sql, catalogReader);
  }

  @Test
  void testCreateView() throws Exception {
    final String sql = "create view dst1 as select * from src1";
    assertFullRoundTripWithIdentityProjectionWorkaround(sql, catalogReader);
  }
}
