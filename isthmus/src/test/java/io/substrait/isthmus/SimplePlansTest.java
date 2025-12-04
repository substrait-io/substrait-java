package io.substrait.isthmus;

import java.io.IOException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

class SimplePlansTest extends PlanTestBase {

  @Test
  void aggFilter() throws IOException, SqlParseException {
    assertProtoPlanRoundrip("select sum(L_ORDERKEY) filter(WHERE L_ORDERKEY > 10) from lineitem ");
  }

  @Test
  void cd() throws IOException, SqlParseException {
    assertProtoPlanRoundrip(
        "select l_partkey, sum(distinct L_ORDERKEY) from lineitem group by l_partkey ");
  }

  @Test
  void filter() throws IOException, SqlParseException {
    assertProtoPlanRoundrip("select * from lineitem WHERE L_ORDERKEY > 10");
  }

  @Test
  void in() throws IOException, SqlParseException {
    assertProtoPlanRoundrip("select * from lineitem WHERE L_ORDERKEY IN (10, 20)");
  }

  @Test
  void joinWithMultiDDLInOneString() throws IOException, SqlParseException {
    assertProtoPlanRoundrip(
        "select * from lineitem l, orders o WHERE o.o_orderkey = l.l_orderkey  and L_ORDERKEY > 10");
  }

  @Test
  void trailingSemicolon() throws IOException, SqlParseException {
    assertProtoPlanRoundrip("select * from lineitem WHERE L_ORDERKEY > 10;");
  }

  @Test
  void isNotNull() throws IOException, SqlParseException {
    assertProtoPlanRoundrip("select L_ORDERKEY from lineitem WHERE L_ORDERKEY is not null;");
  }

  @Test
  void isNull() throws IOException, SqlParseException {
    assertProtoPlanRoundrip("select L_ORDERKEY from lineitem WHERE L_ORDERKEY is null;");
  }

  @Test
  void multiStatement() throws IOException, SqlParseException {
    assertProtoPlanRoundrip(
        "select l_orderkey from lineitem; select l_partkey from lineitem WHERE L_ORDERKEY > 20;",
        new SqlToSubstrait());
  }

  @Test
  void virtualTable() throws IOException, SqlParseException {
    assertProtoPlanRoundrip("SELECT  1");
    assertProtoPlanRoundrip(
        "SELECT  * FROM    ( " + "        VALUES (1), (3) " + "        ) AS q (col1)");
  }
}
