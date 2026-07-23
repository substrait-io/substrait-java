package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.expression.Expression;
import io.substrait.expression.Expression.Switch;
import io.substrait.expression.WindowBound;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.isthmus.SubstraitRelNodeConverter.Context;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.TypeObservation;
import io.substrait.isthmus.expression.TypeObserver;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.Rel.Remap;
import io.substrait.type.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
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

    Project query = sb.project(input -> List.of(expr), sb.emptyVirtualTableScan());

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

    Project query = sb.project(input -> List.of(expr), sb.emptyVirtualTableScan());

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

    Project query = sb.project(input -> List.of(expr), sb.emptyVirtualTableScan());

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

    Project query = sb.project(input -> List.of(expr), sb.emptyVirtualTableScan());

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
  void observeSuppliedAndInferredScalarFunctionTypes() {
    AtomicReference<TypeObservation> observed = new AtomicReference<>();
    ExpressionRexConverter observingConverter = observingConverter(observed::set);
    Expression.ScalarFunctionInvocation expr = add(R.FP32);

    RexNode calciteExpr = expr.accept(observingConverter, Context.newContext());

    TypeObservation observation = observed.get();
    assertEquals(TypeObservation.Source.SCALAR_FUNCTION, observation.source());
    assertSame(expr, observation.expression());
    assertEquals(R.FP32, observation.suppliedType());
    assertTrue(observation.inferenceFailure().isEmpty());
    assertNotEquals(calciteExpr.getType(), observation.inferredType().orElseThrow());
    assertEquals(
        TypeConverter.DEFAULT.toCalcite(typeFactory, R.I32),
        observation.inferredType().orElseThrow());
  }

  @Test
  void observeMatchingScalarFunctionTypes() {
    AtomicReference<TypeObservation> observed = new AtomicReference<>();
    ExpressionRexConverter observingConverter = observingConverter(observed::set);
    Expression.ScalarFunctionInvocation expr = add(R.I32);

    expr.accept(observingConverter, Context.newContext());

    assertSame(expr, observed.get().expression());
    assertEquals(R.I32, observed.get().suppliedType());
    assertEquals(
        TypeConverter.DEFAULT.toCalcite(typeFactory, R.I32),
        observed.get().inferredType().orElseThrow());
  }

  @Test
  void reportScalarInferenceFailureWithoutFailingConversion() {
    AtomicReference<TypeObservation> observed = new AtomicReference<>();
    ExpressionRexConverter observingConverter =
        observingConverter(failingInferenceScalarFunctionConverter(), observed::set);
    Expression.ScalarFunctionInvocation expr = add(R.I32);

    RexNode calciteExpr = expr.accept(observingConverter, Context.newContext());

    assertEquals(TypeConverter.DEFAULT.toCalcite(typeFactory, R.I32), calciteExpr.getType());
    assertSame(expr, observed.get().expression());
    assertEquals(R.I32, observed.get().suppliedType());
    assertTrue(observed.get().inferredType().isEmpty());
    IllegalStateException failure =
        assertInstanceOf(
            IllegalStateException.class, observed.get().inferenceFailure().orElseThrow());
    assertEquals("controlled inference failure", failure.getMessage());
  }

  @Test
  void observeVariadicConcatOnce() {
    List<TypeObservation> observations = new ArrayList<>();
    ExpressionRexConverter observingConverter = observingConverter(observations::add);
    Expression.ScalarFunctionInvocation expr =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_STRING,
            "concat:str",
            R.I32,
            sb.str("a"),
            sb.str("b"),
            sb.str("c"));

    RexNode calciteExpr = expr.accept(observingConverter, Context.newContext());

    assertEquals(1, observations.size());
    assertSame(expr, observations.get(0).expression());
    assertEquals(R.I32, observations.get(0).suppliedType());
    assertEquals(TypeConverter.DEFAULT.toCalcite(typeFactory, R.I32), calciteExpr.getType());
    assertEquals(
        SqlTypeName.VARCHAR, observations.get(0).inferredType().orElseThrow().getSqlTypeName());
  }

  @Test
  void injectTypeObserverThroughConverterProvider() {
    AtomicReference<TypeObservation> observed = new AtomicReference<>();
    ConverterProvider observingProvider =
        new ConverterProvider() {
          @Override
          public TypeObserver getTypeObserver() {
            return observed::set;
          }
        };
    Expression.ScalarFunctionInvocation expr =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC,
            "add:i32_i32",
            R.I32,
            sb.i32(7),
            sb.i32(42));
    Project query = sb.project(input -> List.of(expr), sb.emptyVirtualTableScan());

    new SubstraitToCalcite(observingProvider).convert(query);

    assertEquals(R.I32, observed.get().suppliedType());
    assertEquals(
        TypeConverter.DEFAULT.toCalcite(typeFactory, R.I32),
        observed.get().inferredType().orElseThrow());
  }

  @Test
  void observeEachNestedScalarFunction() {
    List<TypeObservation> observations = new ArrayList<>();
    ExpressionRexConverter observingConverter = observingConverter(observations::add);
    Expression.ScalarFunctionInvocation inner = add(R.I32);
    Expression.ScalarFunctionInvocation outer =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "add:i32_i32", R.I32, inner, sb.i32(3));

    outer.accept(observingConverter, Context.newContext());

    assertEquals(2, observations.size());
    assertTrue(observations.stream().anyMatch(observation -> observation.expression() == inner));
    assertTrue(observations.stream().anyMatch(observation -> observation.expression() == outer));
    assertTrue(
        observations.stream().allMatch(observation -> observation.inferredType().isPresent()));
  }

  @Test
  void propagateObserverException() {
    ExpressionRexConverter observingConverter =
        observingConverter(
            observation -> {
              throw new IllegalStateException("observer failure");
            });
    Expression.ScalarFunctionInvocation expr = add(R.I32);

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () -> expr.accept(observingConverter, Context.newContext()));

    assertEquals("observer failure", failure.getMessage());
  }

  @Test
  void propagateObserverExceptionAfterInferenceFailure() {
    ExpressionRexConverter observingConverter =
        observingConverter(
            failingInferenceScalarFunctionConverter(),
            observation -> {
              throw new IllegalStateException("failure observer failure");
            });
    Expression.ScalarFunctionInvocation expr = add(R.I32);

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () -> expr.accept(observingConverter, Context.newContext()));

    assertEquals("failure observer failure", failure.getMessage());
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

  private Expression.ScalarFunctionInvocation add(Type outputType) {
    return sb.scalarFn(
        DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC,
        "add:i32_i32",
        outputType,
        sb.i32(7),
        sb.i32(42));
  }

  private ExpressionRexConverter observingConverter(TypeObserver observer) {
    return observingConverter(
        new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory), observer);
  }

  private ExpressionRexConverter observingConverter(
      ScalarFunctionConverter scalarFunctionConverter, TypeObserver observer) {
    return new ExpressionRexConverter(
        typeFactory,
        scalarFunctionConverter,
        new WindowFunctionConverter(extensions.windowFunctions(), typeFactory),
        TypeConverter.DEFAULT,
        observer);
  }

  private ScalarFunctionConverter failingInferenceScalarFunctionConverter() {
    SqlFunction failingOperator =
        new SqlFunction(
            "controlled_inference_failure",
            SqlKind.OTHER_FUNCTION,
            binding -> {
              throw new IllegalStateException("controlled inference failure");
            },
            null,
            null,
            SqlFunctionCategory.USER_DEFINED_FUNCTION);
    return new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory) {
      @Override
      public Optional<SqlOperator> getSqlOperatorFromSubstraitFunc(String key, Type outputType) {
        return Optional.of(failingOperator);
      }
    };
  }
}
