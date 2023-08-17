package io.substrait.isthmus;

import java.io.IOException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class WindowFunctionTest extends PlanTestBase {

  @Nested
  class WindowFunctionInvocations {

    @Test
    void rowNumber() throws IOException, SqlParseException {
      assertProtoPlanRoundrip("select O_ORDERKEY, row_number() over () from ORDERS");
    }

    @ParameterizedTest
    @ValueSource(strings = {"rank", "dense_rank", "percent_rank"})
    void rankFunctions(String rankFunction) throws IOException, SqlParseException {
      var query =
          String.format(
              "select O_ORDERKEY, %s() over (order by O_SHIPPRIORITY) from ORDERS", rankFunction);
      assertProtoPlanRoundrip(query);
    }

    @ParameterizedTest
    @ValueSource(strings = {"rank", "dense_rank", "percent_rank"})
    void rankFunctionsWithPartitions(String rankFunction) throws IOException, SqlParseException {
      var query =
          String.format(
              "select O_ORDERKEY, %s() over (partition by O_CUSTKEY order by O_SHIPPRIORITY) from ORDERS",
              rankFunction);
      assertProtoPlanRoundrip(query);
    }

    @Test
    void cumeDist() throws IOException, SqlParseException {
      assertProtoPlanRoundrip(
          "select O_ORDERKEY, cume_dist() over (order by O_SHIPPRIORITY) from ORDERS");
    }

    @Test
    @Disabled
    void ntile() throws IOException, SqlParseException {
      // TODO: The WindowFunctionConverter has some assumptions about function arguments that need
      // to be addressed for this to work.
      assertProtoPlanRoundrip("select O_ORDERKEY, ntile(4) over () from ORDERS");
    }
  }

  @Nested
  class BoundRoundTripping {
    // Calcite is clever and will elide bounds if they are not needed. The following test queries
    // are such that bounds will be included to better verify round-tripping.
    //
    // Plan summaries are included to show that bounds are included. They were generated using the
    // static RelOptUtil.toString(RelNode rel) method with a debugger.

    @Test
    void unbounded() throws IOException, SqlParseException {
      /*
      LogicalProject(EXPR$0=[MAX($7) OVER ()])
        LogicalTableScan(table=[[ORDERS]])
      */
      assertProtoPlanRoundrip("select max(O_SHIPPRIORITY) over () from ORDERS");
    }

    @Test
    void unboundedPreceding() throws IOException, SqlParseException {
      /*
      LogicalProject(EXPR$0=[MIN($7) OVER (PARTITION BY $1 ORDER BY $4 ROWS UNBOUNDED PRECEDING)])
        LogicalTableScan(table=[[ORDERS]])
      */
      var overClause = "partition by O_CUSTKEY order by O_ORDERDATE rows unbounded preceding";
      assertProtoPlanRoundrip(
          String.format("select min(O_SHIPPRIORITY) over (%s) from ORDERS", overClause));
    }

    @Test
    void unboundedFollowing() throws IOException, SqlParseException {
      /*
      LogicalProject(EXPR$0=[MAX($7) OVER (PARTITION BY $1 ORDER BY $4 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)])
        LogicalTableScan(table=[[ORDERS]])
      */
      var overClaus =
          "partition by O_CUSTKEY order by O_ORDERDATE rows between current row AND unbounded following";
      assertProtoPlanRoundrip(
          String.format("select max(O_SHIPPRIORITY) over (%s) from ORDERS", overClaus));
    }

    @Test
    void rowsPrecedingToCurrent() throws IOException, SqlParseException {
      /*
      LogicalProject(EXPR$0=[MIN($7) OVER (PARTITION BY $1 ORDER BY $4 ROWS 1 PRECEDING)])
        LogicalTableScan(table=[[ORDERS]])
      */
      var overClause =
          "partition by O_CUSTKEY order by O_ORDERDATE rows between 1 preceding and current row";
      assertProtoPlanRoundrip(
          String.format("select min(O_SHIPPRIORITY) over (%s) from ORDERS", overClause));
    }

    @Test
    void currentToRowsFollowing() throws IOException, SqlParseException {
      /*
      LogicalProject(EXPR$0=[MAX($7) OVER (PARTITION BY $1 ORDER BY $4 ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING)])
        LogicalTableScan(table=[[ORDERS]])
      */
      var overClause =
          "partition by O_CUSTKEY order by O_ORDERDATE rows between current row and 2 following";
      assertProtoPlanRoundrip(
          String.format("select max(O_SHIPPRIORITY) over (%s) from ORDERS", overClause));
    }

    @Test
    void rowsPrecedingAndFollowing() throws IOException, SqlParseException {
      /*
      LogicalProject(EXPR$0=[MIN($7) OVER (PARTITION BY $1 ORDER BY $4 ROWS BETWEEN 3 PRECEDING AND 4 FOLLOWING)])
       LogicalTableScan(table=[[ORDERS]])
      */
      var overClause =
          "partition by O_CUSTKEY order by O_ORDERDATE  rows between 3 preceding and 4 following";
      assertProtoPlanRoundrip(
          String.format("select min(O_SHIPPRIORITY) over (%s) from ORDERS", overClause));
    }
  }

  @Nested
  class AggregateFunctionInvocations {

    @ParameterizedTest
    @ValueSource(strings = {"avg", "count", "max", "min", "sum"})
    void standardAggregateFunctions(String aggFunction) throws SqlParseException, IOException {
      assertProtoPlanRoundrip(
          String.format(
              "select %s(L_LINENUMBER) over (partition BY L_PARTKEY) from lineitem", aggFunction));
    }
  }
}
