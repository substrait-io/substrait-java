package io.substrait.isthmus;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.substrait.proto.Plan;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

public class OneWaySimpleTest extends PlanTestBase {
  @Test
  public void windowFunctionsSql2Sub() throws IOException, SqlParseException {
    SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
    List<String> ddlStmts = Lists.newArrayList(asString("tpch/schema.sql"));
    print(
        sqlToSubstrait.execute(
            "select s_name,s_address, sum(s_nationkey) over (partition by s_address order by s_phone desc) as rnk from supplier where s_nationkey > 10",
            ddlStmts));
    /**
     * wait for the substrait update. print( sqlToSubstrait.execute( "select s_name,s_address,
     * row_number() over (partition by s_address order by s_phone desc) as rnk from supplier where
     * s_nationkey > 10", ddlStmts));
     */
  }

  private void print(Plan plan) {
    try {
      System.out.println(JsonFormat.printer().includingDefaultValueFields().print(plan));
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
