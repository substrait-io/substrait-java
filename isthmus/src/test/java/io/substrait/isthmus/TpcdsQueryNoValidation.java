package io.substrait.isthmus;

import com.google.protobuf.util.JsonFormat;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.calcite.adapter.tpcds.TpcdsSchema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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

  static final Set<Integer> EXCLUDED = Set.of(9, 27, 36, 70, 86, 98);

  static IntStream testCases() {
    return IntStream.rangeClosed(1, 99).filter(n -> !EXCLUDED.contains(n));
  }

  /**
   * This test only validates that generating substrait plans for TPC-DS queries does not fail. As
   * of now this test does not validate correctness of the generated plan
   */
  @ParameterizedTest
  @MethodSource("testCases")
  void testQuery(int i) throws Exception {
    SqlToSubstrait s = new SqlToSubstrait();
    TpcdsSchema schema = new TpcdsSchema(1.0);
    String sql = asString(String.format("tpcds/queries/%02d.sql", i));
    var plan = s.execute(sql, "tpcds", schema);
    System.out.println(JsonFormat.printer().print(plan));
  }
}
