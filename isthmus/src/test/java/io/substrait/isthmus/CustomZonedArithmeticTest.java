package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.substrait.expression.Expression;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Year/month arithmetic on a zoned timestamp when another extension declares it without the
 * timezone argument of the datetime extension. Each declaration keeps its own calls.
 */
class CustomZonedArithmeticTest extends PlanTestBase {

  private static final String URN = "extension:test:custom_zoned_arithmetic";

  private static final String IMPLS =
      "    impls:\n"
          + "      - args:\n"
          + "          - name: x\n"
          + "            value: precision_timestamp_tz<P>\n"
          + "          - name: y\n"
          + "            value: interval_year\n"
          + "        return: precision_timestamp_tz<P>\n";

  private static final String YAML =
      "%YAML 1.2\n---\nurn: "
          + URN
          + "\nscalar_functions:\n  - name: \"add\"\n"
          + IMPLS
          + "  - name: \"subtract\"\n"
          + IMPLS;

  CustomZonedArithmeticTest() {
    super(
        ConverterProvider.builder()
            .extensions(
                DefaultExtensionCatalog.DEFAULT_COLLECTION.merge(SimpleExtension.load(YAML)))
            .build());
  }

  @ParameterizedTest
  @ValueSource(strings = {"add", "subtract"})
  void customCallKeepsItsDeclaration(String name) {
    assertFullRoundTrip(
        sb.project(
            input ->
                List.of(
                    sb.scalarFn(
                        URN,
                        name + ":ptstz_iyear",
                        R.precisionTimestampTZ(6),
                        sb.fieldReference(input, 0),
                        sb.fieldReference(input, 1))),
            sb.remap(2),
            scan()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"add", "subtract"})
  void standardCallKeepsItsTimezone(String name) {
    assertFullRoundTrip(
        sb.project(
            input ->
                List.of(
                    sb.scalarFn(
                        DefaultExtensionCatalog.FUNCTIONS_DATETIME,
                        name + ":ptstz_iyear_str",
                        R.precisionTimestampTZ(6),
                        sb.fieldReference(input, 0),
                        sb.fieldReference(input, 1),
                        sb.currentTimezone())),
            sb.remap(2),
            scan()));
  }

  @ParameterizedTest
  @CsvSource({"+, add:ptstz_iyear", "-, subtract:ptstz_iyear"})
  void sqlSelectsTheCustomDeclaration(String operator, String key) throws Exception {
    Rel rel =
        new SqlToSubstrait(converterProvider)
            .convert(
                "SELECT ts " + operator + " INTERVAL '2' YEAR FROM t",
                SubstraitCreateStatementParser.processCreateStatementsToCatalog(
                    "CREATE TABLE t (ts TIMESTAMP(6) WITH LOCAL TIME ZONE NOT NULL)"))
            .getRoots()
            .get(0)
            .getInput();
    Expression.ScalarFunctionInvocation call =
        assertInstanceOf(
            Expression.ScalarFunctionInvocation.class,
            assertInstanceOf(Project.class, rel).getExpressions().get(0));
    assertEquals(URN, call.declaration().urn());
    assertEquals(key, call.declaration().key());
  }

  private Rel scan() {
    return sb.namedScan(
        List.of("t"), List.of("ts", "iv"), List.of(R.precisionTimestampTZ(6), R.INTERVAL_YEAR));
  }
}
