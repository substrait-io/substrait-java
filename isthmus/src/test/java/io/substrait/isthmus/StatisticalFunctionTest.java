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
