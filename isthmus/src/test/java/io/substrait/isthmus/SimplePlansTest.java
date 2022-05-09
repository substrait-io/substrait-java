package io.substrait.isthmus;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.substrait.proto.Plan;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

public class SimplePlansTest extends PlanTestBase {

  @Test
  public void aggFilter() throws IOException, SqlParseException {
    SqlToSubstrait s = new SqlToSubstrait();
    String[] values = asString("tpch/schema.sql").split(";");
    var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();
    s.execute("select sum(L_ORDERKEY) filter(WHERE L_ORDERKEY > 10) from lineitem ", creates);
  }

  @Test
  public void cd() throws IOException, SqlParseException {
    SqlToSubstrait s = new SqlToSubstrait();
    String[] values = asString("tpch/schema.sql").split(";");
    var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();
    // creates.forEach(System.out::println);
    s.execute(
        "select l_partkey, sum(distinct L_ORDERKEY) from lineitem group by l_partkey ", creates);
  }

  @Test
  public void filter() throws IOException, SqlParseException {
    SqlToSubstrait s = new SqlToSubstrait();
    String[] values = asString("tpch/schema.sql").split(";");
    var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();
    // creates.forEach(System.out::println);
    print(s.execute("select * from lineitem WHERE L_ORDERKEY > 10", creates));
  }

  @Test
  public void joinWithMultiDDLInOneString() throws IOException, SqlParseException {
    SqlToSubstrait s = new SqlToSubstrait();
    List<String> ddlStmts = Lists.newArrayList(asString("tpch/schema.sql"));
    print(
        s.execute(
            "select * from lineitem l, orders o WHERE o.o_orderkey = l.l_orderkey  and L_ORDERKEY > 10",
            ddlStmts));
  }

  private void print(Plan plan) {
    try {
      System.out.println(JsonFormat.printer().includingDefaultValueFields().print(plan));
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
