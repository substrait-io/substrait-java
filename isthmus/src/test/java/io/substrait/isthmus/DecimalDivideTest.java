package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.expression.Expression;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.plan.Plan;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.relation.Project;
import io.substrait.type.Type;
import java.util.List;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelRoot;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class DecimalDivideTest extends PlanTestBase {
  @ParameterizedTest
  @CsvSource({
    "10, 2, 5, 1, 17, 8",
    "10, 2, 9, 0, 20, 12",
    "28, 10, 10, 2, 38, 18",
    "38, 10, 38, 10, 38, 6",
    "32, 0, 1, 0, 38, 6"
  })
  void exportUsesDivisorScale(int p1, int s1, int p2, int s2, int precision, int scale)
      throws Exception {
    CalciteCatalogReader catalog = catalog(p1, s1, p2, s2);
    Plan exported = new SqlToSubstrait().convert("SELECT a / b FROM t", catalog);
    Project project = (Project) exported.getRoots().get(0).getInput();
    Expression.ScalarFunctionInvocation divide =
        (Expression.ScalarFunctionInvocation) project.getExpressions().get(0);
    Type expected = N.decimal(precision, scale);

    assertEquals("divide:dec_dec", divide.declaration().key());
    assertEquals(expected, divide.getType());
    assertEquals(List.of(expected), project.getRecordType().fields());
    assertProtoPlanRoundrip("SELECT a / b FROM t", new SqlToSubstrait(), catalog);
  }

  @ParameterizedTest
  @CsvSource({
    "10, 2, 5, 1, 21, 8",
    "10, 2, 9, 0, 29, 12",
    "28, 10, 10, 2, 38, 10",
    "38, 10, 38, 10, 38, 6"
  })
  void importPreservesPreviouslyDeclaredResultType(
      int p1, int s1, int p2, int s2, int oldPrecision, int oldScale) throws Exception {
    CalciteCatalogReader catalog = catalog(p1, s1, p2, s2);
    Plan plan = new SqlToSubstrait().convert("SELECT a / b FROM t", catalog);
    Plan.Root root = plan.getRoots().get(0);
    Project project = (Project) root.getInput();
    Expression.ScalarFunctionInvocation divide =
        (Expression.ScalarFunctionInvocation) project.getExpressions().get(0);
    Type oldType = N.decimal(oldPrecision, oldScale);
    // Existing plans carry the result type derived before the formula was corrected.
    Expression oldDivide =
        Expression.ScalarFunctionInvocation.builder().from(divide).outputType(oldType).build();
    Project oldProject = Project.builder().from(project).expressions(List.of(oldDivide)).build();
    Plan.Root oldRoot = Plan.Root.builder().from(root).input(oldProject).build();
    Plan oldPlan = Plan.builder().from(plan).roots(List.of(oldRoot)).build();

    io.substrait.proto.Plan wire = toProto(oldPlan);
    Plan imported =
        new ProtoPlanConverter(extensions)
            .from(io.substrait.proto.Plan.parseFrom(wire.toByteArray()));
    assertEquals(wire, toProto(imported));
    assertEquals(List.of(oldType), imported.getRoots().get(0).getInput().getRecordType().fields());

    RelRoot calcite =
        new SubstraitToCalcite(converterProvider, catalog).convert(imported.getRoots().get(0));
    assertEquals(
        oldType,
        TypeConverter.DEFAULT.toSubstrait(
            calcite.validatedRowType.getFieldList().get(0).getType()));
    Plan.Root reexported = SubstraitRelVisitor.convert(calcite, converterProvider);
    Project reexportedProject = (Project) reexported.getInput();
    assertEquals(List.of(oldType), reexportedProject.getRecordType().fields());
    Expression.ScalarFunctionInvocation reexportedDivide =
        (Expression.ScalarFunctionInvocation) reexportedProject.getExpressions().get(0);
    assertEquals("divide:dec_dec", reexportedDivide.declaration().key());
    assertEquals(oldType, reexportedDivide.getType());
  }

  private CalciteCatalogReader catalog(int p1, int s1, int p2, int s2) throws Exception {
    return SubstraitCreateStatementParser.processCreateStatementsToCatalog(
        String.format("CREATE TABLE t (a DECIMAL(%d,%d), b DECIMAL(%d,%d))", p1, s1, p2, s2));
  }
}
