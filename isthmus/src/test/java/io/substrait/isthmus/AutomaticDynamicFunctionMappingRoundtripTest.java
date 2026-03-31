package io.substrait.isthmus;

import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.plan.Plan;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Roundtrip test for the AutomaticDynamicFunctionMappingConverterProvider feature.
 *
 * <p>This test verifies that: 1. Substrait plans using unmapped functions (like strftime or
 * regexp_match_substring from extensions) are successfully converted. 2. With
 * AutomaticDynamicFunctionMappingConverterProvider enabled, these unmapped functions are
 * dynamically mapped to Calcite operators. 3. The roundtrip conversion (Substrait → Calcite →
 * Substrait) is stable, including for SQL queries.
 *
 * <p>The test uses unmapped functions like strftime and regexp_match_substring that are defined in
 * extension YAML but not in FunctionMappings.
 */
class AutomaticDynamicFunctionMappingRoundtripTest extends PlanTestBase {

  AutomaticDynamicFunctionMappingRoundtripTest() {
    super(new AutomaticDynamicFunctionMappingConverterProvider());
  }

  /**
   * Test roundtrip with multiple variants of the same unmapped function.
   *
   * <p>This test builds a Substrait plan with multiple variants of an unmapped function (e.g.
   * strftime with a precision timestamp and strftime with a date) using SubstraitBuilder, then
   * verifies that the framework correctly maps both to the same generic operator and resolves them
   * back to their correct specific variants during roundtrip conversions.
   */
  @Test
  void testMultipleVariantsOfUnmappedFunctionRoundtrip() {
    // Build table scan with a precision timestamp and a date
    NamedScan table =
        sb.namedScan(List.of("t"), List.of("ts", "dt"), List.of(R.precisionTimestamp(6), R.DATE));

    // Create project with multiple variants of strftime
    Project project =
        sb.project(
            input ->
                List.of(
                    // First variant: strftime(ts, '%Y-%m-%d')
                    sb.scalarFn(
                        DefaultExtensionCatalog.FUNCTIONS_DATETIME,
                        "strftime:pts_str",
                        R.STRING,
                        sb.fieldReference(input, 0),
                        ExpressionCreator.string(false, "%Y-%m-%d")),
                    // Second variant: strftime(dt, '%Y-%m-%d')
                    sb.scalarFn(
                        DefaultExtensionCatalog.FUNCTIONS_DATETIME,
                        "strftime:date_str",
                        R.STRING,
                        sb.fieldReference(input, 1),
                        ExpressionCreator.string(false, "%Y-%m-%d"))),
            sb.remap(2, 3), // The inputs are indices 0, 1. The two scalarFn outputs are 2, 3.
            table);

    // Build plan with output field names and default execution behavior
    Plan plan =
        sb.plan(Plan.Root.builder().input(project).names(List.of("ts_str", "dt_str")).build());

    // Use PlanTestBase helper method for comprehensive roundtrip testing
    assertFullRoundTrip(plan.getRoots().get(0));
  }

  /**
   * Test roundtrip with SQL query using unmapped strftime function.
   *
   * <p>This test verifies that SQL queries with unmapped functions (strftime from
   * functions_datetime.yaml) can be parsed, validated, and converted to Substrait and back when
   * AutomaticDynamicFunctionMappingConverterProvider is used. The provider populates the operator
   * table with unmapped function signatures, allowing SqlValidator to accept them during SQL
   * parsing.
   */
  @Test
  void testUnmappedStrftimeSqlRoundtrip() throws Exception {
    String createStatements = "CREATE TABLE t (ts TIMESTAMP)";
    String query = "SELECT strftime(ts, '%Y-%m-%d') FROM t";

    // Perform roundtrip: SQL → Substrait → Calcite → Substrait → Calcite → Substrait
    // Uses loose POJO comparison since dynamic function mapping may transform the structure
    assertSqlSubstraitRelRoundTripLoosePojoComparison(
        query, SubstraitCreateStatementParser.processCreateStatementsToCatalog(createStatements));
  }

  /**
   * Test roundtrip with SQL query using multiple unmapped functions.
   *
   * <p>This test verifies that SQL queries with multiple unmapped function calls (like
   * regexp_match_substring) can be handled when AutomaticDynamicFunctionMappingConverterProvider is
   * used. The operator table is populated with unmapped function signatures, allowing occurrences
   * of unmapped functions to be recognized during SQL parsing and conversion.
   */
  @Test
  void testMultipleUnmappedFunctionsSqlRoundtrip() throws Exception {
    String createStatements = "CREATE TABLE t (date_str VARCHAR, ts_str VARCHAR)";
    String query =
        "SELECT regexp_match_substring(date_str, '^[0-9]{4}') AS parsed_date, regexp_match_substring(ts_str, '^[0-9]{4}') AS parsed_ts FROM t";

    // Perform roundtrip with multiple unmapped function calls
    assertSqlSubstraitRelRoundTripLoosePojoComparison(
        query, SubstraitCreateStatementParser.processCreateStatementsToCatalog(createStatements));
  }

  /**
   * Test roundtrip with multiple calls to the same unmapped function.
   *
   * <p>This test verifies that an unmapped function can be called multiple times in the same query
   * and all calls are properly converted and matched. This ensures the function matching logic
   * works correctly even with repeated function calls.
   */
  @Test
  void testMultipleCallsToSameFunctionSqlRoundtrip() throws Exception {
    String createStatements = "CREATE TABLE t (ts TIMESTAMP)";
    String query =
        "SELECT strftime(ts, '%Y-%m-%d') AS formatted1, strftime(ts, '%H:%M:%S') AS formatted2 FROM t";

    // Perform roundtrip with multiple calls to the same function
    assertSqlSubstraitRelRoundTripLoosePojoComparison(
        query, SubstraitCreateStatementParser.processCreateStatementsToCatalog(createStatements));
  }
}
