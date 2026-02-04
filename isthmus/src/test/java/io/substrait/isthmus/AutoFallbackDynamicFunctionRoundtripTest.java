package io.substrait.isthmus;

import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.plan.Plan;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Roundtrip test for the autoFallbackToDynamicFunctionMapping feature.
 *
 * <p>This test verifies that: 1. Substrait plans using unmapped functions (like strftime from
 * extensions) are built with SubstraitBuilder 2. With autoFallbackToDynamicFunctionMapping enabled,
 * these unmapped functions are dynamically mapped to Calcite operators 3. The roundtrip conversion
 * (Substrait → Calcite → Substrait) is stable, including for SQL queries
 *
 * <p>The test uses unmapped datetime functions like strftime that are defined in extension YAML but
 * not in FunctionMappings.
 */
class AutoFallbackDynamicFunctionRoundtripTest extends PlanTestBase {

  AutoFallbackDynamicFunctionRoundtripTest() {
    super(DefaultExtensionCatalog.DEFAULT_COLLECTION);
  }

  /**
   * Test roundtrip with unmapped strftime function using SubstraitBuilder.
   *
   * <p>This test builds a Substrait plan using SubstraitBuilder that calls the unmapped
   * strftime:ts_str function (which exists in functions_datetime.yaml but NOT in FunctionMappings).
   */
  @Test
  void testUnmappedStrftimeRoundtrip() {

    // Build table scan
    NamedScan table = substraitBuilder.namedScan(List.of("t"), List.of("ts"), List.of(R.TIMESTAMP));

    // Create project with unmapped strftime function call
    Project project =
        substraitBuilder.project(
            input ->
                List.of(
                    // Call unmapped strftime function: strftime(ts, '%Y-%m-%d')
                    substraitBuilder.scalarFn(
                        DefaultExtensionCatalog.FUNCTIONS_DATETIME,
                        "strftime:ts_str",
                        R.STRING,
                        substraitBuilder.fieldReference(input, 0),
                        ExpressionCreator.string(false, "%Y-%m-%d"))),
            table);

    // Build plan with output field names
    // Note: Project creates a struct with 2 fields internally, so we need 2 names
    Plan plan =
        Plan.builder()
            .roots(
                List.of(
                    Plan.Root.builder()
                        .input(project)
                        .names(List.of("formatted_date", "_ignored"))
                        .build()))
            .build();

    // Enable autoFallbackToDynamicFunctionMapping
    FeatureBoard featureBoard =
        ImmutableFeatureBoard.builder().autoFallbackToDynamicFunctionMapping(true).build();

    // Use PlanTestBase helper method for comprehensive roundtrip testing with FeatureBoard
    assertFullRoundTrip(plan.getRoots().get(0), featureBoard);
  }

  /**
   * Test roundtrip with multiple unmapped functions from default extensions.
   *
   * <p>This test builds Substrait plans with multiple unmapped functions from the default
   * extensions (functions_datetime.yaml) using SubstraitBuilder, then verifies that
   * autoFallbackToDynamicFunctionMapping enables dynamic mapping of these functions during
   * Substrait → Calcite → Substrait roundtrip conversions.
   */
  @Test
  void testMultipleUnmappedFunctionsRoundtrip() {
    // Build table scan
    NamedScan table = substraitBuilder.namedScan(List.of("t"), List.of("ts"), List.of(R.TIMESTAMP));

    // Create project with multiple unmapped strftime function calls from default extensions
    Project project =
        substraitBuilder.project(
            input ->
                List.of(
                    // First unmapped strftime call: strftime(ts, '%Y-%m-%d')
                    substraitBuilder.scalarFn(
                        DefaultExtensionCatalog.FUNCTIONS_DATETIME,
                        "strftime:ts_str",
                        R.STRING,
                        substraitBuilder.fieldReference(input, 0),
                        ExpressionCreator.string(false, "%Y-%m-%d")),
                    // Second unmapped strftime call: strftime(ts, '%H:%M:%S')
                    substraitBuilder.scalarFn(
                        DefaultExtensionCatalog.FUNCTIONS_DATETIME,
                        "strftime:ts_str",
                        R.STRING,
                        substraitBuilder.fieldReference(input, 0),
                        ExpressionCreator.string(false, "%H:%M:%S"))),
            table);

    // Build plan with output field names
    // Project creates a struct with 3 fields internally (input field + 2 output fields)
    Plan plan =
        Plan.builder()
            .roots(
                List.of(
                    Plan.Root.builder()
                        .input(project)
                        .names(List.of("date_str", "time_str", "_ignored"))
                        .build()))
            .build();

    // Enable autoFallbackToDynamicFunctionMapping
    FeatureBoard featureBoard =
        ImmutableFeatureBoard.builder().autoFallbackToDynamicFunctionMapping(true).build();

    // Use PlanTestBase helper method for comprehensive roundtrip testing with FeatureBoard
    assertFullRoundTrip(plan.getRoots().get(0), featureBoard);
  }

  /**
   * Test roundtrip with SQL query using unmapped strftime function.
   *
   * <p>This test verifies that SQL queries with unmapped functions (strftime from
   * functions_datetime.yaml) can be parsed, validated, and converted to Substrait and back when
   * autoFallbackToDynamicFunctionMapping is enabled. The feature flag should enable the
   * SqlToSubstrait converter to populate its operator table with unmapped function signatures,
   * allowing SqlValidator to accept them during SQL parsing.
   */
  @Test
  void testUnmappedStrftimeSqlRoundtrip() throws Exception {
    String createStatements = "CREATE TABLE t (ts TIMESTAMP)";
    String query = "SELECT strftime(ts, '%Y-%m-%d') FROM t";

    // Enable autoFallbackToDynamicFunctionMapping to populate operator table with unmapped
    // functions
    FeatureBoard featureBoard =
        ImmutableFeatureBoard.builder().autoFallbackToDynamicFunctionMapping(true).build();

    // Perform roundtrip: SQL → Substrait → Calcite → Substrait → Calcite → Substrait
    // Uses loose POJO comparison since dynamic function mapping may transform the structure
    assertSqlSubstraitRelRoundTripLoosePojoComparison(query, createStatements, featureBoard);
  }

  /**
   * Test roundtrip with SQL query using multiple unmapped functions.
   *
   * <p>This test verifies that SQL queries with multiple unmapped function calls can be handled
   * when autoFallbackToDynamicFunctionMapping is enabled. The operator table is populated with
   * unmapped function signatures, allowing all occurrences of the unmapped functions to be
   * recognized during SQL parsing and conversion.
   */
  @Test
  void testMultipleUnmappedFunctionsSqlRoundtrip() throws Exception {
    String createStatements = "CREATE TABLE t (date_str VARCHAR, ts_str VARCHAR)";
    String query =
        "SELECT strptime_date(date_str, '%Y-%m-%d') AS parsed_date, strptime_timestamp(ts_str, '%Y-%m-%d %H:%M:%S') AS parsed_ts FROM t";

    // Enable autoFallbackToDynamicFunctionMapping
    FeatureBoard featureBoard =
        ImmutableFeatureBoard.builder().autoFallbackToDynamicFunctionMapping(true).build();

    // Perform roundtrip with multiple unmapped function calls
    assertSqlSubstraitRelRoundTripLoosePojoComparison(query, createStatements, featureBoard);
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

    // Enable autoFallbackToDynamicFunctionMapping
    FeatureBoard featureBoard =
        ImmutableFeatureBoard.builder().autoFallbackToDynamicFunctionMapping(true).build();

    // Perform roundtrip with multiple calls to the same function
    assertSqlSubstraitRelRoundTripLoosePojoComparison(query, createStatements, featureBoard);
  }
}
