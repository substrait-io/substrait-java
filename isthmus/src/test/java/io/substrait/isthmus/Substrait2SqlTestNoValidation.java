package io.substrait.isthmus;

import static io.substrait.isthmus.SqlToSubstrait.EXTENSION_COLLECTION;

import io.substrait.relation.Rel;
import java.util.Arrays;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlDialect;
import org.junit.jupiter.api.Test;

public class Substrait2SqlTestNoValidation extends PlanTestBase {
  @Test
  public void simpleTest() throws Exception {
    String query = "select p_size + 100 from part where p_partkey > 100";
    test(query);
  }

  @Test
  public void simpleTest2() throws Exception {
    String query = "select l_partkey, l_discount from lineitem where l_orderkey > 100";
    test(query);
  }

  @Test
  public void simpleTest3() throws Exception {
    String query =
        "select l_partkey + l_orderkey, l_shipdate from lineitem where l_shipdate < date '1998-01-01' ";
    test(query);
  }

  private void test(String query) throws Exception {
    String[] values = asString("tpch/schema.sql").split(";");
    var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();

    SqlToSubstrait s = new SqlToSubstrait();
    RelRoot relRoot = s.sqlToRelNode(query, creates);
    Rel pojoRel = SubstraitRelVisitor.convert(relRoot, EXTENSION_COLLECTION);

    RelNode relnodeRoot = new SubstraitToSql().convert(pojoRel, creates);

    System.out.println(
        SubstraitToSql.toSql(relnodeRoot, SqlDialect.DatabaseProduct.SNOWFLAKE.getDialect()));
  }
}
