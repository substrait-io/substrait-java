package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.isthmus.operation.CalciteOperation;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.relation.Rel;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

class DdlRoundtripTest extends PlanTestBase {
  final Prepare.CatalogReader catalogReader =
      SubstraitCreateStatementParser.processCreateStatementsToCatalog(
          "create table src1 (intcol int, charcol varchar(10))",
          "create table src2 (intcol int, charcol varchar(10))");

  public DdlRoundtripTest() throws SqlParseException {
    super();
  }

  void testSqlToSubstrait(String sqlStatement) throws SqlParseException {
    SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
    io.substrait.proto.Plan protoPlan = sqlToSubstrait.execute(sqlStatement, catalogReader);
    Plan plan = new ProtoPlanConverter().from(protoPlan);
    io.substrait.proto.Plan protoPlan1 = new PlanProtoConverter().toProto(plan);
    assertEquals(protoPlan, protoPlan1);
  }

  protected void assertCalciteOperationSubstraitRelRoundTrip(
      String query, Prepare.CatalogReader catalogReader) throws Exception {
    // sql <--> substrait round trip test.
    // Assert (sql -> calcite -> substrait) and (sql -> substrait -> calcite -> substrait) are same.
    // Return list of sql -> Substrait rel -> Calcite rel.
    SqlToSubstrait s = new SqlToSubstrait();

    // 1. SQL -> Substrait Plan
    Plan plan1 = s.convert(query, catalogReader);

    // 2. Substrait Plan  -> Substrait Rel
    Plan.Root pojo1 = plan1.getRoots().get(0);

    // 3. Substrait Rel -> CalciteOperation

    SubstraitToCalciteOperation substraitToCalciteOperation =
        new SubstraitToCalciteOperation(extensions, typeFactory, catalogReader, s.parserConfig);
    Rel rel = plan1.getRoots().get(0).getInput();
    CalciteOperation calciteOperation =
        rel.accept(substraitToCalciteOperation, SubstraitRelNodeConverter.Context.newContext());

    // 4. CalciteOperation -> Substrait Rel
    CalciteOperationToSubstrait calciteOperationToSubstrait =
        new CalciteOperationToSubstrait(extensions, s.featureBoard);
    Plan.Root pojo2 = calciteOperation.accept(calciteOperationToSubstrait);

    assertEquals(pojo1, pojo2);
  }

  void testPlanRoundTrip(String sqlStatement) throws Exception {
    SqlToSubstrait sql2subst = new SqlToSubstrait();
    final Plan plan = sql2subst.convert(sqlStatement, catalogReader);

    assertPlanRoundtrip(plan);
    assertCalciteOperationSubstraitRelRoundTrip(sqlStatement, catalogReader);

    assertFullRoundTrip(sqlStatement, catalogReader);
  }

  @Test
  void testCreateTable() throws Exception {
    String sql = "create table dst1 as select * from src1";
    testSqlToSubstrait(sql);
    // TBD: full roundtrip is not possible because there is no relational algebra for DDL
    testPlanRoundTrip(sql);
  }

  @Test
  void testCreateView() throws Exception {
    String sql = "create view dst1 as select * from src1";
    testSqlToSubstrait(sql);
    testPlanRoundTrip(sql);
  }
}
