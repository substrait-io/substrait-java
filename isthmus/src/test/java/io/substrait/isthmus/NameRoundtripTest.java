package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.isthmus.sql.SubstraitSqlToCalcite;
import io.substrait.plan.Plan;
import io.substrait.relation.NamedScan;
import java.util.List;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.junit.jupiter.api.Test;

public class NameRoundtripTest extends PlanTestBase {

  private static final SimpleExtension.ExtensionCollection EXTENSION_COLLECTION =
      SimpleExtension.loadDefaults();

  @Test
  void preserveNamesFromSql() throws Exception {
    String createStatement = "CREATE TABLE foo(a BIGINT, b BIGINT)";
    CalciteCatalogReader catalogReader =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(createStatement);

    SubstraitToCalcite substraitToCalcite =
        new SubstraitToCalcite(EXTENSION_COLLECTION, typeFactory);

    String query = "SELECT \"a\", \"B\" FROM foo GROUP BY a, b";
    List<String> expectedNames = List.of("a", "B");

    org.apache.calcite.rel.RelRoot calciteRelRoot1 =
        SubstraitSqlToCalcite.convertQuery(query, catalogReader);

    assertEquals(expectedNames, calciteRelRoot1.validatedRowType.getFieldNames());

    io.substrait.plan.Plan.Root substraitRelRoot =
        SubstraitRelVisitor.convert(calciteRelRoot1, EXTENSION_COLLECTION);
    assertEquals(expectedNames, substraitRelRoot.getNames());

    org.apache.calcite.rel.RelRoot calciteRelRoot2 = substraitToCalcite.convert(substraitRelRoot);
    assertEquals(expectedNames, calciteRelRoot2.validatedRowType.getFieldNames());
  }

  @Test
  void preserveNamesFromSubstrait() {
    NamedScan rel =
        substraitBuilder.namedScan(
            List.of("foo"),
            List.of("i64", "struct", "struct0", "struct1"),
            List.of(R.I64, R.struct(R.FP64, R.STRING)));

    Plan.Root planRoot =
        Plan.Root.builder().input(rel).names(List.of("i", "s", "s0", "s1")).build();
    assertFullRoundTrip(planRoot);
  }
}
