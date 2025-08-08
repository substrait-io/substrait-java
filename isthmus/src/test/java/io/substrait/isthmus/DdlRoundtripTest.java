package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.plan.ImmutablePlan;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import java.util.List;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

public class DdlRoundtripTest extends PlanTestBase {
  final Prepare.CatalogReader catalogReader =
      SubstraitCreateStatementParser.processCreateStatementsToCatalog(
          "create table src1 (intcol int, charcol varchar(10))",
          "create table src2 (intcol int, charcol varchar(10))");

  public DdlRoundtripTest() throws SqlParseException {}

  void testSqlToSubstrait(String sqlStatement) throws SqlParseException {
    SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
    io.substrait.proto.Plan protoPlan = sqlToSubstrait.execute(sqlStatement, catalogReader);
    Plan plan = new ProtoPlanConverter().from(protoPlan);
    io.substrait.proto.Plan protoPlan1 = new PlanProtoConverter().toProto(plan);
    assertEquals(protoPlan, protoPlan1);
  }

  void testPlanRoundTrip(String sqlStatement) throws SqlParseException {
    SqlToSubstrait sql2subst = new SqlToSubstrait();
    final List<io.substrait.plan.Plan.Root> relRoots =
        sql2subst.sqlToPlanNodes(sqlStatement, catalogReader);
    ImmutablePlan.Builder builder = io.substrait.plan.Plan.builder();
    for (final io.substrait.plan.Plan.Root planRoot : relRoots) {
      builder.addRoots(planRoot);
    }
    final Plan plan = builder.build();
    assertPlanRoundtrip(plan);
  }

  @Test
  void testCreateTable() throws SqlParseException {
    String sql = "create table dst1 as select * from src1";
    testSqlToSubstrait(sql);
    // TBD: full roundtrip is not possible because there is no relational algebra for DDL
    testPlanRoundTrip(sql);
  }

  @Test
  void testCreateView() throws SqlParseException {
    String sql = "create view dst1 as select * from src1";
    testSqlToSubstrait(sql);
    testPlanRoundTrip(sql);
  }
}
