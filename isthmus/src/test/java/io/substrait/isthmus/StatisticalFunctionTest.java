package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.EnumArg;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.isthmus.sql.SubstraitSqlToCalcite;
import io.substrait.plan.Plan;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Rel;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Verifies that the SQL statistical aggregates (STDDEV_POP/SAMP, VAR_POP/SAMP) map to the Substrait
 * {@code std_dev} / {@code variance} functions using the non-deprecated enum-argument signatures
 * ({@code std_dev:req_fp64} etc.), carrying the population/sample distinction as a {@code
 * distribution} {@link EnumArg} rather than a function option.
 */
class StatisticalFunctionTest extends PlanTestBase {

  static final String CREATES =
      "CREATE TABLE numbers (i8 TINYINT, i16 SMALLINT, i32 INT, i64 BIGINT, fp32 REAL, fp64 DOUBLE)";

  @ParameterizedTest
  @CsvSource({"STDDEV_POP", "STDDEV_SAMP", "VAR_POP", "VAR_SAMP"})
  void roundTrip(String fn) throws Exception {
    assertFullRoundTrip(String.format("SELECT %s(fp32), %s(fp64) FROM numbers", fn, fn), CREATES);
  }

  // Integer arguments are cast to fp64 (and the result cast back) since std_dev/variance only have
  // fp32/fp64 signatures. The cast rewrite stacks a cast projection over the aggregate's input
  // projection, which Calcite merges (and prunes the redundant column) when it rebuilds the plan on
  // the Substrait -> Calcite leg. The plan straight from SQL therefore differs from the one after a
  // Calcite -> Substrait -> Calcite round trip, so these compare loosely: they assert stability of
  // the normalized plan rather than equality with the initial plan.

  @ParameterizedTest
  @CsvSource({"STDDEV_POP", "STDDEV_SAMP", "VAR_POP", "VAR_SAMP"})
  void roundTripIntegerInput(String fn) throws Exception {
    assertSqlSubstraitRelRoundTripLoosePojoComparison(
        String.format("SELECT %s(i32) FROM numbers", fn),
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(CREATES));
  }

  @Test
  void roundTripIntegerInputSharedWithOtherAggregate() throws Exception {
    // The integer column is shared by SUM (which must keep operating on the integer) and STDDEV_POP
    // (which is cast to fp64); the cast must be appended, not applied in place.
    assertSqlSubstraitRelRoundTripLoosePojoComparison(
        "SELECT SUM(i32), STDDEV_POP(i32) FROM numbers",
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(CREATES));
  }

  @Test
  void roundTripIntegerInputWithGrouping() throws Exception {
    assertSqlSubstraitRelRoundTripLoosePojoComparison(
        "SELECT i8, VAR_POP(i32) FROM numbers GROUP BY i8",
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(CREATES));
  }

  @ParameterizedTest
  @CsvSource({
    "STDDEV_POP, std_dev, POPULATION",
    "STDDEV_SAMP, std_dev, SAMPLE",
    "VAR_POP, variance, POPULATION",
    "VAR_SAMP, variance, SAMPLE",
  })
  void usesEnumArgSignature(String sqlFn, String substraitFn, String distribution)
      throws Exception {
    Prepare.CatalogReader catalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(CREATES);
    RelRoot calcite =
        SubstraitSqlToCalcite.convertQuery(
            String.format("SELECT %s(fp64) FROM numbers", sqlFn),
            catalog,
            converterProvider.getSqlOperatorTable());
    Plan.Root root = SubstraitRelVisitor.convert(calcite, converterProvider);

    AggregateFunctionInvocation function = firstMeasure(root.getInput()).getFunction();

    // The non-deprecated enum-arg variant is used (note the "req" enum argument in the key).
    assertEquals(substraitFn + ":req_fp64", function.declaration().key());

    // The distribution is carried as a leading EnumArg, not as a function option.
    List<io.substrait.expression.FunctionArg> args = function.arguments();
    EnumArg distributionArg = assertInstanceOf(EnumArg.class, args.get(0));
    assertEquals(Optional.of(distribution), distributionArg.value());
    assertTrue(function.options().isEmpty(), "expected no function options");
  }

  /** Recursively finds the first {@link Aggregate} measure in the relation tree. */
  private static Aggregate.Measure firstMeasure(Rel rel) {
    if (rel instanceof Aggregate) {
      Aggregate aggregate = (Aggregate) rel;
      if (!aggregate.getMeasures().isEmpty()) {
        return aggregate.getMeasures().get(0);
      }
    }
    for (Rel input : rel.getInputs()) {
      Aggregate.Measure measure = firstMeasure(input);
      if (measure != null) {
        return measure;
      }
    }
    return null;
  }
}
