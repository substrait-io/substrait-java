package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.expression.Expression;
import io.substrait.expression.WindowBound;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.relation.Rel;
import java.io.IOException;
import java.util.List;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class WindowFunctionTest extends PlanTestBase {

  @Nested
  class WindowFunctionInvocations {

    @Test
    void rowNumber() throws IOException, SqlParseException {
      assertFullRoundTrip("select O_ORDERKEY, row_number() over () from ORDERS");
    }

    @Test
    void lag() throws IOException, SqlParseException {
      assertFullRoundTrip("select O_TOTALPRICE, LAG(O_TOTALPRICE, 1) over () from ORDERS");
    }

    @Test
    void lead() throws IOException, SqlParseException {
      assertFullRoundTrip("select O_TOTALPRICE, LEAD(O_TOTALPRICE, 1) over () from ORDERS");
    }

    @ParameterizedTest
    @ValueSource(strings = {"rank", "dense_rank", "percent_rank"})
    void rankFunctions(final String rankFunction) throws IOException, SqlParseException {
      final String query =
          String.format(
              "select O_ORDERKEY, %s() over (order by O_SHIPPRIORITY) from ORDERS", rankFunction);
      assertFullRoundTrip(query);
    }

    @ParameterizedTest
    @ValueSource(strings = {"rank", "dense_rank", "percent_rank"})
    void rankFunctionsWithPartitions(final String rankFunction)
        throws IOException, SqlParseException {
      final String query =
          String.format(
              "select O_ORDERKEY, %s() over (partition by O_CUSTKEY order by O_SHIPPRIORITY) from ORDERS",
              rankFunction);
      assertFullRoundTrip(query);
    }

    @Test
    void cumeDist() throws IOException, SqlParseException {
      assertFullRoundTrip(
          "select O_ORDERKEY, cume_dist() over (order by O_SHIPPRIORITY) from ORDERS");
    }

    @Test
    void ntile() throws IOException, SqlParseException {
      assertFullRoundTrip("select O_ORDERKEY, ntile(4) over () from ORDERS");
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
      assertFullRoundTrip("select max(O_SHIPPRIORITY) over () from ORDERS");
    }

    @Test
    void unboundedPreceding() throws IOException, SqlParseException {
      /*
      LogicalProject(EXPR$0=[MIN($7) OVER (PARTITION BY $1 ORDER BY $4 ROWS UNBOUNDED PRECEDING)])
        LogicalTableScan(table=[[ORDERS]])
      */
      final String overClause =
          "partition by O_CUSTKEY order by O_ORDERDATE rows unbounded preceding";
      assertFullRoundTrip(
          String.format("select min(O_SHIPPRIORITY) over (%s) from ORDERS", overClause));
    }

    @Test
    void unboundedFollowing() throws IOException, SqlParseException {
      /*
      LogicalProject(EXPR$0=[MAX($7) OVER (PARTITION BY $1 ORDER BY $4 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)])
        LogicalTableScan(table=[[ORDERS]])
      */
      final String overClaus =
          "partition by O_CUSTKEY order by O_ORDERDATE rows between current row AND unbounded following";
      assertFullRoundTrip(
          String.format("select max(O_SHIPPRIORITY) over (%s) from ORDERS", overClaus));
    }

    @Test
    void rowsPrecedingToCurrent() throws IOException, SqlParseException {
      /*
      LogicalProject(EXPR$0=[MIN($7) OVER (PARTITION BY $1 ORDER BY $4 ROWS 1 PRECEDING)])
        LogicalTableScan(table=[[ORDERS]])
      */
      final String overClause =
          "partition by O_CUSTKEY order by O_ORDERDATE rows between 1 preceding and current row";
      assertFullRoundTrip(
          String.format("select min(O_SHIPPRIORITY) over (%s) from ORDERS", overClause));
    }

    @Test
    void currentToRowsFollowing() throws IOException, SqlParseException {
      /*
      LogicalProject(EXPR$0=[MAX($7) OVER (PARTITION BY $1 ORDER BY $4 ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING)])
        LogicalTableScan(table=[[ORDERS]])
      */
      final String overClause =
          "partition by O_CUSTKEY order by O_ORDERDATE rows between current row and 2 following";
      assertFullRoundTrip(
          String.format("select max(O_SHIPPRIORITY) over (%s) from ORDERS", overClause));
    }

    @Test
    void rowsPrecedingAndFollowing() throws IOException, SqlParseException {
      /*
      LogicalProject(EXPR$0=[MIN($7) OVER (PARTITION BY $1 ORDER BY $4 ROWS BETWEEN 3 PRECEDING AND 4 FOLLOWING)])
       LogicalTableScan(table=[[ORDERS]])
      */
      final String overClause =
          "partition by O_CUSTKEY order by O_ORDERDATE  rows between 3 preceding and 4 following";
      assertFullRoundTrip(
          String.format("select min(O_SHIPPRIORITY) over (%s) from ORDERS", overClause));
    }

    @Test
    void rangePrecedingToCurrent() throws IOException, SqlParseException {
      /*
      LogicalProject(EXPR$0=[MIN($7) OVER (PARTITION BY $1 ORDER BY $3 RANGE 10 PRECEDING)])
        LogicalTableScan(table=[[ORDERS]])
      */
      final String overClause =
          "partition by O_CUSTKEY order by O_TOTALPRICE range between 10 preceding and current row";
      assertFullRoundTrip(
          String.format("select min(O_SHIPPRIORITY) over (%s) from ORDERS", overClause));
    }

    @Test
    void rangeCurrentToFollowing() throws IOException, SqlParseException {
      /*
      LogicalProject(EXPR$0=[MIN($7) OVER (PARTITION BY $1 ORDER BY $3 RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING)])
        LogicalTableScan(table=[[ORDERS]])
      */
      final String overClause =
          "partition by O_CUSTKEY order by O_TOTALPRICE range between current row and 11 following";
      assertFullRoundTrip(
          String.format("select min(O_SHIPPRIORITY) over (%s) from ORDERS", overClause));
    }
  }

  @Nested
  class AggregateFunctionInvocations {

    @ParameterizedTest
    @ValueSource(strings = {"avg", "count", "max", "min", "sum"})
    void standardAggregateFunctions(final String aggFunction)
        throws SqlParseException, IOException {
      assertFullRoundTrip(
          String.format(
              "select %s(L_LINENUMBER) over (partition BY L_PARTKEY) from lineitem", aggFunction));
    }
  }

  @Test
  void rejectQueriesWithIgnoreNulls() {
    // IGNORE NULLS cannot be specified in the Substrait representation.
    // Queries using it should be rejected.
    final String query = "select last_value(L_LINENUMBER) ignore nulls over () from lineitem";
    assertThrows(IllegalArgumentException.class, () -> assertFullRoundTrip(query));
  }

  @ParameterizedTest
  @ValueSource(strings = {"lag", "lead"})
  void lagLeadFunctions(final String function) {
    final Rel rel =
        substraitBuilder.project(
            input ->
                List.of(
                    substraitBuilder.windowFn(
                        DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC,
                        String.format("%s:any", function),
                        R.FP64,
                        Expression.AggregationPhase.INITIAL_TO_RESULT,
                        Expression.AggregationInvocation.ALL,
                        Expression.WindowBoundsType.ROWS,
                        WindowBound.Preceding.UNBOUNDED,
                        WindowBound.Following.CURRENT_ROW,
                        substraitBuilder.fieldReference(input, 0))),
            substraitBuilder.remap(1),
            substraitBuilder.namedScan(List.of("window_test"), List.of("a"), List.of(R.FP64)));

    assertFullRoundTrip(rel);
  }

  @ParameterizedTest
  @ValueSource(strings = {"lag", "lead"})
  void lagLeadWithOffset(final String function) {
    final Rel rel =
        substraitBuilder.project(
            input ->
                List.of(
                    substraitBuilder.windowFn(
                        DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC,
                        String.format("%s:any_i32", function),
                        R.FP64,
                        Expression.AggregationPhase.INITIAL_TO_RESULT,
                        Expression.AggregationInvocation.ALL,
                        Expression.WindowBoundsType.RANGE,
                        WindowBound.Preceding.UNBOUNDED,
                        WindowBound.Following.UNBOUNDED,
                        substraitBuilder.fieldReference(input, 0),
                        substraitBuilder.i32(1))),
            substraitBuilder.remap(1),
            substraitBuilder.namedScan(List.of("window_test"), List.of("a"), List.of(R.FP64)));

    assertFullRoundTrip(rel);
  }

  @ParameterizedTest
  @ValueSource(strings = {"lag", "lead"})
  void lagLeadWithOffsetAndDefault(final String function) {
    final Rel rel =
        substraitBuilder.project(
            input ->
                List.of(
                    substraitBuilder.windowFn(
                        DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC,
                        String.format("%s:any_i32_any", function),
                        R.I64,
                        Expression.AggregationPhase.INITIAL_TO_RESULT,
                        Expression.AggregationInvocation.ALL,
                        Expression.WindowBoundsType.ROWS,
                        WindowBound.Preceding.UNBOUNDED,
                        WindowBound.Following.CURRENT_ROW,
                        substraitBuilder.fieldReference(input, 0),
                        substraitBuilder.i32(1),
                        substraitBuilder.fp64(100.0))),
            substraitBuilder.remap(1),
            substraitBuilder.namedScan(List.of("window_test"), List.of("a"), List.of(R.FP64)));

    assertFullRoundTrip(rel);
  }
}
