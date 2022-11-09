package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

public class SimplePlansTest extends PlanTestBase {

  @Test
  public void aggFilter() throws IOException, SqlParseException {
    assertProtoPlanRoundrip("select sum(L_ORDERKEY) filter(WHERE L_ORDERKEY > 10) from lineitem ");
  }

  @Test
  public void cd() throws IOException, SqlParseException {
    assertProtoPlanRoundrip(
        "select l_partkey, sum(distinct L_ORDERKEY) from lineitem group by l_partkey ");
  }

  @Test
  public void filter() throws IOException, SqlParseException {
    assertProtoPlanRoundrip("select * from lineitem WHERE L_ORDERKEY > 10");
  }

  @Test
  public void joinWithMultiDDLInOneString() throws IOException, SqlParseException {
    assertProtoPlanRoundrip(
        "select * from lineitem l, orders o WHERE o.o_orderkey = l.l_orderkey  and L_ORDERKEY > 10");
  }

  @Test
  public void trailingSemicolon() throws IOException, SqlParseException {
    assertProtoPlanRoundrip("select * from lineitem WHERE L_ORDERKEY > 10;");
  }

  @Test
  public void isNotNull() throws IOException, SqlParseException {
    assertProtoPlanRoundrip("select L_ORDERKEY from lineitem WHERE L_ORDERKEY is not null;");
  }

  @Test
  public void isNull() throws IOException, SqlParseException {
    assertProtoPlanRoundrip("select L_ORDERKEY from lineitem WHERE L_ORDERKEY is null;");
  }

  @Test
  public void multiStatement() throws IOException, SqlParseException {
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          assertProtoPlanRoundrip(
              "select l_orderkey from lineitem; select l_partkey from lineitem WHERE L_ORDERKEY > 20;");
        },
        "SQL must contain only a single statement");
    var features = ImmutableFeatureBoard.builder().allowsSqlBatch(true).build();
    assertProtoPlanRoundrip(
        "select l_orderkey from lineitem; select l_partkey from lineitem WHERE L_ORDERKEY > 20;",
        new SqlToSubstrait(features));
  }

  @Test
  public void virtualTable() throws IOException, SqlParseException {
    assertProtoPlanRoundrip("SELECT  1");
    assertProtoPlanRoundrip(
        "SELECT  * FROM    ( " + "        VALUES (1), (3) " + "        ) AS q (col1)");
  }
}
