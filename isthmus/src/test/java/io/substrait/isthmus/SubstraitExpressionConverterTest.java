package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.junit.jupiter.api.Test;

class SubstraitExpressionConverterTest extends PlanTestBase {

  final ExpressionRexConverter converter;

  final List<Type> commonTableType = List.of(R.I32, R.FP32, N.STRING, N.BOOLEAN);
  final Rel commonTable =
      sb.namedScan(List.of("example"), List.of("a", "b", "c", "d"), commonTableType);

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
    Switch expr =
        sb.switchExpression(
            sb.fieldReference(commonTable, 0),
            List.of(sb.switchClause(sb.i32(0), sb.fieldReference(commonTable, 3))),
            sb.bool(false));
    RexNode calciteExpr = expr.accept(converter, Context.newContext());

    assertTypeMatch(calciteExpr.getType(), N.BOOLEAN);
  }

  @Test
  void scalarSubQuery() {
    Rel subQueryRel = createSubQueryRel();

    Expression.ScalarSubquery expr =
        Expression.ScalarSubquery.builder().type(R.I64).input(subQueryRel).build();

    Project query = sb.project(input -> List.of(expr), sb.emptyScan());

    RelNode calciteRel = substraitToCalcite.convert(query);

    assertInstanceOf(LogicalProject.class, calciteRel);
    List<RexNode> calciteProjectExpr = ((LogicalProject) calciteRel).getProjects();
    assertEquals(1, calciteProjectExpr.size());
    assertEquals(SqlKind.SCALAR_QUERY, calciteProjectExpr.get(0).getKind());
  }

  @Test
  void existsSetPredicate() {
    Rel subQueryRel = createSubQueryRel();

    Expression.SetPredicate expr =
        Expression.SetPredicate.builder()
            .predicateOp(Expression.PredicateOp.PREDICATE_OP_EXISTS)
            .tuples(subQueryRel)
            .build();

    Project query = sb.project(input -> List.of(expr), sb.emptyScan());

    RelNode calciteRel = substraitToCalcite.convert(query);

    assertInstanceOf(LogicalProject.class, calciteRel);
    List<RexNode> calciteProjectExpr = ((LogicalProject) calciteRel).getProjects();
    assertEquals(1, calciteProjectExpr.size());
    assertEquals(SqlKind.EXISTS, calciteProjectExpr.get(0).getKind());
  }

  @Test
  void uniqueSetPredicate() {
    Rel subQueryRel = createSubQueryRel();

    Expression.SetPredicate expr =
        Expression.SetPredicate.builder()
            .predicateOp(Expression.PredicateOp.PREDICATE_OP_UNIQUE)
            .tuples(subQueryRel)
            .build();

    Project query = sb.project(input -> List.of(expr), sb.emptyScan());

    RelNode calciteRel = substraitToCalcite.convert(query);

    assertInstanceOf(LogicalProject.class, calciteRel);
    List<RexNode> calciteProjectExpr = ((LogicalProject) calciteRel).getProjects();
    assertEquals(1, calciteProjectExpr.size());
    assertEquals(SqlKind.UNIQUE, calciteProjectExpr.get(0).getKind());
  }

  @Test
  void unspecifiedSetPredicate() {
    Rel subQueryRel = createSubQueryRel();

    Expression.SetPredicate expr =
        Expression.SetPredicate.builder()
            .predicateOp(Expression.PredicateOp.PREDICATE_OP_UNSPECIFIED)
            .tuples(subQueryRel)
            .build();

    Project query = sb.project(input -> List.of(expr), sb.emptyScan());

    Exception exception =
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
    return sb.project(
        input -> List.of(sb.fieldReference(input, 0)),
        Remap.of(List.of(3)),
        sb.filter(input -> sb.equal(sb.fieldReference(input, 2), sb.str("EUROPE")), commonTable));
  }

  @Test
  void useSubstraitReturnTypeDuringScalarFunctionConversion() {
    Expression.ScalarFunctionInvocation expr =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC,
            "add:i32_i32",
            // THIS IS (INTENTIONALLY) THE WRONG OUTPUT TYPE
            // SHOULD BE R.I32
            R.FP32,
            sb.i32(7),
            sb.i32(42));

    RexNode calciteExpr = expr.accept(expressionRexConverter, Context.newContext());
    assertEquals(TypeConverter.DEFAULT.toCalcite(typeFactory, R.FP32), calciteExpr.getType());
  }

  @Test
  void useSubstraitReturnTypeDuringWindowFunctionConversion() {
    Expression.WindowFunctionInvocation expr =
        sb.windowFn(
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
            sb.i32(42));

    RexNode calciteExpr = expr.accept(expressionRexConverter, Context.newContext());
    assertEquals(TypeConverter.DEFAULT.toCalcite(typeFactory, R.STRING), calciteExpr.getType());
  }

  void assertTypeMatch(RelDataType actual, Type expected) {
    Type type = TypeConverter.DEFAULT.toSubstrait(actual);
    assertEquals(expected, type);
  }
}
