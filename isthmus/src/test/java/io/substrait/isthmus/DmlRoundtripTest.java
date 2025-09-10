package io.substrait.isthmus;

import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

public class DmlRoundtripTest extends PlanTestBase {

  final Prepare.CatalogReader catalogReader =
      SubstraitCreateStatementParser.processCreateStatementsToCatalog(
          "create table src1 (intcol int, charcol varchar(10))",
          "create table src2 (intcol int, charcol varchar(10))");

  public DmlRoundtripTest() throws SqlParseException {}

  @Test
  void testDelete() throws SqlParseException {
    assertFullRoundTripWorkaroundOptimizer("delete from src1 where intcol=10", catalogReader);
  }

  @Test
  void testUpdate() throws SqlParseException {
    assertFullRoundTripWorkaroundOptimizer(
        "update src1 set intcol=10 where charcol='a'", catalogReader);
  }

  @Test
  void testInsert() throws SqlParseException {
    assertFullRoundTripWorkaroundOptimizer(
        "insert into src1 (intcol, charcol) values (1,'a'); ", catalogReader);
    assertFullRoundTripWorkaroundOptimizer(
        "insert into src1 (intcol, charcol) select intcol,charcol from src2;", catalogReader);
  }
}
