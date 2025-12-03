package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.expression.Expression.Switch;
import io.substrait.expression.WindowBound;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.isthmus.SubstraitRelNodeConverter.Context;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
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

class SubstraitExpressionConverterTest extends PlanTestBase {

  static final TypeCreator R = TypeCreator.of(false);
  static final TypeCreator N = TypeCreator.of(true);

  final SubstraitBuilder b = new SubstraitBuilder(extensions);

  final ExpressionRexConverter converter;

  final List<Type> commonTableType = List.of(R.I32, R.FP32, N.STRING, N.BOOLEAN);
  final Rel commonTable =
      b.namedScan(List.of("example"), List.of("a", "b", "c", "d"), commonTableType);

  final SubstraitRelNodeConverter relNodeConverter =
      new SubstraitRelNodeConverter(extensions, typeFactory, builder);

  final ExpressionRexConverter expressionRexConverter =
      new ExpressionRexConverter(
          typeFactory,
          new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory),
          new WindowFunctionConverter(extensions.windowFunctions(), typeFactory),
          TypeConverter.DEFAULT);

  public SubstraitExpressionConverterTest() {
    converter = relNodeConverter.expressionRexConverter;
  }

  @Test
  void switchExpression() {
    final Switch expr =
        b.switchExpression(
            b.fieldReference(commonTable, 0),
            List.of(b.switchClause(b.i32(0), b.fieldReference(commonTable, 3))),
            b.bool(false));
    final RexNode calciteExpr = expr.accept(converter, Context.newContext());

    assertTypeMatch(calciteExpr.getType(), N.BOOLEAN);
  }

  @Test
  void scalarSubQuery() {
    final Rel subQueryRel = createSubQueryRel();

    final Expression.ScalarSubquery expr =
        Expression.ScalarSubquery.builder().type(R.I64).input(subQueryRel).build();

    final Project query = b.project(input -> List.of(expr), b.emptyScan());

    final SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);
    final RelNode calciteRel = substraitToCalcite.convert(query);

    assertInstanceOf(LogicalProject.class, calciteRel);
    final List<RexNode> calciteProjectExpr = ((LogicalProject) calciteRel).getProjects();
    assertEquals(1, calciteProjectExpr.size());
    assertEquals(SqlKind.SCALAR_QUERY, calciteProjectExpr.get(0).getKind());
  }

  @Test
  void existsSetPredicate() {
    final Rel subQueryRel = createSubQueryRel();

    final Expression.SetPredicate expr =
        Expression.SetPredicate.builder()
            .predicateOp(Expression.PredicateOp.PREDICATE_OP_EXISTS)
            .tuples(subQueryRel)
            .build();

    final Project query = b.project(input -> List.of(expr), b.emptyScan());

    final SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);
    final RelNode calciteRel = substraitToCalcite.convert(query);

    assertInstanceOf(LogicalProject.class, calciteRel);
    final List<RexNode> calciteProjectExpr = ((LogicalProject) calciteRel).getProjects();
    assertEquals(1, calciteProjectExpr.size());
    assertEquals(SqlKind.EXISTS, calciteProjectExpr.get(0).getKind());
  }

  @Test
  void uniqueSetPredicate() {
    final Rel subQueryRel = createSubQueryRel();

    final Expression.SetPredicate expr =
        Expression.SetPredicate.builder()
            .predicateOp(Expression.PredicateOp.PREDICATE_OP_UNIQUE)
            .tuples(subQueryRel)
            .build();

    final Project query = b.project(input -> List.of(expr), b.emptyScan());

    final SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);
    final RelNode calciteRel = substraitToCalcite.convert(query);

    assertInstanceOf(LogicalProject.class, calciteRel);
    final List<RexNode> calciteProjectExpr = ((LogicalProject) calciteRel).getProjects();
    assertEquals(1, calciteProjectExpr.size());
    assertEquals(SqlKind.UNIQUE, calciteProjectExpr.get(0).getKind());
  }

  @Test
  void unspecifiedSetPredicate() {
    final Rel subQueryRel = createSubQueryRel();

    final Expression.SetPredicate expr =
        Expression.SetPredicate.builder()
            .predicateOp(Expression.PredicateOp.PREDICATE_OP_UNSPECIFIED)
            .tuples(subQueryRel)
            .build();

    final Project query = b.project(input -> List.of(expr), b.emptyScan());

    final SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);
    final Exception exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> {
              substraitToCalcite.convert(query);
            });

    assertEquals(
        "Cannot handle SetPredicate when PredicateOp is PREDICATE_OP_UNSPECIFIED.",
        exception.getMessage());
  }

  /**
   * Creates a Substrait {@link Rel} equivalent to the following SQL query:
   *
   * <p>select a from example where c = 'EUROPE'
   *
   * @return the Substrait {@link Rel} equivalent of the above SQL query
   */
  Rel createSubQueryRel() {
    return b.project(
        input -> List.of(b.fieldReference(input, 0)),
        Remap.of(List.of(3)),
        b.filter(input -> b.equal(b.fieldReference(input, 2), b.str("EUROPE")), commonTable));
  }

  @Test
  void useSubstraitReturnTypeDuringScalarFunctionConversion() {
    final Expression.ScalarFunctionInvocation expr =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC,
            "add:i32_i32",
            // THIS IS (INTENTIONALLY) THE WRONG OUTPUT TYPE
            // SHOULD BE R.I32
            R.FP32,
            b.i32(7),
            b.i32(42));

    final RexNode calciteExpr = expr.accept(expressionRexConverter, Context.newContext());
    assertEquals(TypeConverter.DEFAULT.toCalcite(typeFactory, R.FP32), calciteExpr.getType());
  }

  @Test
  void useSubstraitReturnTypeDuringWindowFunctionConversion() {
    final Expression.WindowFunctionInvocation expr =
        b.windowFn(
            DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC,
            "row_number:",
            // THIS IS (INTENTIONALLY) THE WRONG OUTPUT TYPE
            // SHOULD BE R.I64
            R.STRING,
            Expression.AggregationPhase.INITIAL_TO_RESULT,
            Expression.AggregationInvocation.ALL,
            Expression.WindowBoundsType.RANGE,
            WindowBound.UNBOUNDED,
            WindowBound.UNBOUNDED,
            b.i32(42));

    final RexNode calciteExpr = expr.accept(expressionRexConverter, Context.newContext());
    assertEquals(TypeConverter.DEFAULT.toCalcite(typeFactory, R.STRING), calciteExpr.getType());
  }

  void assertTypeMatch(final RelDataType actual, final Type expected) {
    final Type type = TypeConverter.DEFAULT.toSubstrait(actual);
    assertEquals(expected, type);
  }
}
