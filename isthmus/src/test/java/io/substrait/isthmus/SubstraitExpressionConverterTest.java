package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.Rel.Remap;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.junit.jupiter.api.Test;

public class SubstraitExpressionConverterTest extends PlanTestBase {

  static final TypeCreator R = TypeCreator.of(false);
  static final TypeCreator N = TypeCreator.of(true);

  final SubstraitBuilder b = new SubstraitBuilder(extensions);

  final ExpressionRexConverter converter;

  final List<Type> commonTableType = List.of(R.I32, R.FP32, N.STRING, N.BOOLEAN);
  final Rel commonTable =
      b.namedScan(List.of("example"), List.of("a", "b", "c", "d"), commonTableType);

  final SubstraitRelNodeConverter relNodeConverter =
      new SubstraitRelNodeConverter(extensions, typeFactory, builder);

  public SubstraitExpressionConverterTest() {
    converter = relNodeConverter.expressionRexConverter;
  }

  @Test
  public void switchExpression() {
    var expr =
        b.switchExpression(
            b.fieldReference(commonTable, 0),
            List.of(b.switchClause(b.i32(0), b.fieldReference(commonTable, 3))),
            b.bool(false));
    var calciteExpr = expr.accept(converter);

    assertTypeMatch(calciteExpr.getType(), N.BOOLEAN);
  }

  @Test
  public void scalarSubQuery() {
    Rel subQueryRel =
        b.project(
            input -> List.of(b.fieldReference(input, 0)),
            Remap.of(List.of(3)),
            b.filter(
                input ->
                    b.equal(
                        b.fieldReference(input, 2),
                        Expression.StrLiteral.builder().nullable(false).value("EUROPE").build()),
                commonTable));

    Expression.ScalarSubquery expr =
        Expression.ScalarSubquery.builder()
            .type(TypeCreator.REQUIRED.I64)
            .input(subQueryRel)
            .build();

    Project query = b.project(input -> List.of(expr), b.emptyScan());

    SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);
    RelNode calciteRel = substraitToCalcite.convert(query);

    assertInstanceOf(LogicalProject.class, calciteRel);
    List<RexNode> calciteProjectExpr = ((LogicalProject) calciteRel).getProjects();
    assertEquals(1, calciteProjectExpr.size());
    assertEquals(SqlKind.SCALAR_QUERY, calciteProjectExpr.get(0).getKind());
  }

  void assertTypeMatch(RelDataType actual, Type expected) {
    Type type = TypeConverter.DEFAULT.toSubstrait(actual);
    assertEquals(expected, type);
  }
}
