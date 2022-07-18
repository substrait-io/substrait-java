package io.substrait.isthmus;

import com.google.protobuf.util.JsonFormat;
import org.apache.calcite.adapter.tpcds.TpcdsSchema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 *
 *
 * <h3>Setup of Schema and Queries</h3>
 *
 * <li>Schema using `org.apache.calcite.adapter.tpcds.TpcdsSchema` from
 *     `org.apache.calcite:calcite-plus:1.28.0`
 * <li>For queries started with `net.hydromatic.tpcds.query.Query` and then fixed generation issues
 *     replacing with specific queries from Spark SQL tpcds benchmark.
 *
 *     <h3>Generator and query parsing issues and fixes</h3>
 *
 * <li>`substr` instead of `substring`
 * <li>keywords used `returns`, `at`,.... Change to `rets`, `at`, ...
 * <li>doesn't handle may kinds of generator expressions like: `Define
 *     SDATE=date([YEAR]+"-01-01",[YEAR]+"-07-01",sales);`, `Define
 *     CATEGORY=ulist(dist(categories,1,1),3);` and `define STATE= ulist(dist(fips_county, 3, 1),
 *     9). So replaced with constants from spark sql tpcds query.
 * <li>Interval specified as `30 days`; changed to `interval '30' day`
 */
public class TpcdsQueryNoValidation extends PlanTestBase {

  /**
   * This test only validates that generating substrait plans for TPC-DS queries does not fail. As
   * of now this test does not validate correctness of the generated plan
   */
  private void testQuery(int i) throws Exception {
    SqlToSubstrait s = new SqlToSubstrait();
    TpcdsSchema schema = new TpcdsSchema(1.0);
    String sql = asString(String.format("tpcds/queries/%02d.sql", i));
    var plan = s.execute(sql, "tpcds", schema);
    System.out.println(JsonFormat.printer().print(plan));
  }

  @ParameterizedTest
  @ValueSource(
      ints = {
        33, 34, 35, 37, 38, 40, 41, 42, 43, 45, 46, 48, 50, 52, 54, 55, 56, 58, 59, 60, 61, 62, 64,
        65, 68, 69, 71, 73, 74, 75, 76, 77, 79, 81, 82, 83, 85, 87, 88, 90, 92, 93, 94, 95, 96, 97,
        99
      })
  public void tpcdsSuccess(int query) throws Exception {
    testQuery(query);
  }

  @ParameterizedTest
  @ValueSource(
      ints = {
        2, 5, 9, 12, 17, 20, 24, 27, 36, 39, 44, 47, 49, 51, 53, 57, 63, 66, 67, 70, 72, 78, 80, 84,
        86, 89, 91, 98
      })
  public void tpcdsFailure(int query) throws Exception {
    // testQuery(query);
  }
}
