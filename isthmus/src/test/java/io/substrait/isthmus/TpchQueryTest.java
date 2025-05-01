package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.plan.Plan.Root;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.proto.Plan;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** TPC-H test to convert SQL to Substrait and then convert those plans back to SQL. */
public class TpchQueryTest extends PlanTestBase {
  private static final Set<Integer> fromSubstraitExclusions = Set.of(17);

  private final ProtoPlanConverter planConverter = new ProtoPlanConverter();

  static IntStream testCases() {
    return IntStream.rangeClosed(1, 22);
  }

  /**
   * Note that this test does not currently validate the correctness of the Substrait plan; just
   * that the SQL can be converted to Substrait and back to SQL without error.
   */
  @ParameterizedTest
  @MethodSource("testCases")
  public void testQuery(int query) throws IOException {
    String inputSql = asString(String.format("tpch/queries/%02d.sql", query));

    Plan plan = assertDoesNotThrow(() -> toSubstraitPlan(inputSql), "SQL to Substrait");

    if (!fromSubstraitExclusions.contains(query)) {
      assertDoesNotThrow(() -> toSql(plan), "Substrait to SQL");
    }
  }

  private Plan toSubstraitPlan(String sql) throws SqlParseException {
    List<String> createStatements = tpchSchemaCreateStatements();
    return new SqlToSubstrait().execute(sql, createStatements);
  }

  private String toSql(Plan plan) {
    List<Root> roots = planConverter.from(plan).getRoots();
    assertEquals(1, roots.size(), "number of roots");

    Root root = roots.get(0);
    RelRoot relRoot = new SubstraitToCalcite(extensions, typeFactory).convert(root);
    RelNode project = relRoot.project(true);
    return SubstraitToSql.toSql(project);
  }
}
