package io.substrait.isthmus;

import io.substrait.plan.Plan;
import java.io.IOException;
import java.util.List;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

public class SqlToSubstraitTest extends PlanTestBase {

  @Test
  void testDdlDml() throws SqlParseException, IOException {
    final String sqlStatements = asString("sqltosubstrait/sqltosubstrait.sql");

    SqlToSubstrait sql2subst = new SqlToSubstrait();
    final List<io.substrait.plan.Plan.Root> relRoots =
        sql2subst.sqlToPlanNodes(
            sqlStatements,
            List.of(
                "create table src1 (intcol int, charcol varchar(10))",
                "create table src2 (intcol int, charcol varchar(10))"));
    var builder = io.substrait.plan.Plan.builder();
    for (final io.substrait.plan.Plan.Root planRoot : relRoots) {
      builder.addRoots(planRoot);
    }
    final Plan plan = builder.build();
    assertPlanRoundtrip(plan);
  }
}
