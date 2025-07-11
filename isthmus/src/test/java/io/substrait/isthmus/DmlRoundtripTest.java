package io.substrait.isthmus;

import java.util.List;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

public class DmlRoundtripTest extends PlanTestBase {

  final List<String> createStatements =
      List.of(
          "create table src1 (intcol int, charcol varchar(10))",
          "create table src2 (intcol int, charcol varchar(10))");

  @Test
  void testDelete() throws SqlParseException {
    assertFullRoundTrip("delete from src1 where intcol=10", createStatements);
  }

  @Test
  void testUpdate() throws SqlParseException {
    assertFullRoundTrip("update src1 set intcol=10 where charcol='a'", createStatements);
  }

  @Test
  void testInsert() throws SqlParseException {
    assertFullRoundTrip("insert into src1 (intcol, charcol) values (1,'a'); ", createStatements);
    assertFullRoundTrip(
        "insert into src1 (intcol, charcol) select intcol,charcol from src2;", createStatements);
  }
}
