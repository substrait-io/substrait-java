package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.expression.Expression.ScalarFunctionInvocation;
import io.substrait.expression.ImmutableEnumArg;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.Rel.Remap;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
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
    Rel subQueryRel = createSubQueryRel();

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

  @Test
  public void existsSetPredicate() {
    Rel subQueryRel = createSubQueryRel();

    Expression.SetPredicate expr =
        Expression.SetPredicate.builder()
            .predicateOp(Expression.PredicateOp.PREDICATE_OP_EXISTS)
            .tuples(subQueryRel)
            .build();

    Project query = b.project(input -> List.of(expr), b.emptyScan());

    SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);
    RelNode calciteRel = substraitToCalcite.convert(query);

    assertInstanceOf(LogicalProject.class, calciteRel);
    List<RexNode> calciteProjectExpr = ((LogicalProject) calciteRel).getProjects();
    assertEquals(1, calciteProjectExpr.size());
    assertEquals(SqlKind.EXISTS, calciteProjectExpr.get(0).getKind());
  }

  @Test
  public void uniqueSetPredicate() {
    Rel subQueryRel = createSubQueryRel();

    Expression.SetPredicate expr =
        Expression.SetPredicate.builder()
            .predicateOp(Expression.PredicateOp.PREDICATE_OP_UNIQUE)
            .tuples(subQueryRel)
            .build();

    Project query = b.project(input -> List.of(expr), b.emptyScan());

    SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);
    RelNode calciteRel = substraitToCalcite.convert(query);

    assertInstanceOf(LogicalProject.class, calciteRel);
    List<RexNode> calciteProjectExpr = ((LogicalProject) calciteRel).getProjects();
    assertEquals(1, calciteProjectExpr.size());
    assertEquals(SqlKind.UNIQUE, calciteProjectExpr.get(0).getKind());
  }

  @Test
  public void unspecifiedSetPredicate() {
    Rel subQueryRel = createSubQueryRel();

    Expression.SetPredicate expr =
        Expression.SetPredicate.builder()
            .predicateOp(Expression.PredicateOp.PREDICATE_OP_UNSPECIFIED)
            .tuples(subQueryRel)
            .build();

    Project query = b.project(input -> List.of(expr), b.emptyScan());

    SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);
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
    return b.project(
        input -> List.of(b.fieldReference(input, 0)),
        Remap.of(List.of(3)),
        b.filter(
            input ->
                b.equal(
                    b.fieldReference(input, 2),
                    Expression.StrLiteral.builder().nullable(false).value("EUROPE").build()),
            commonTable));
  }

  void assertTypeMatch(RelDataType actual, Type expected) {
    Type type = TypeConverter.DEFAULT.toSubstrait(actual);
    assertEquals(expected, type);
  }

  @Test
  public void extractTimestampTzScalarFunction() {
    ScalarFunctionInvocation reqTstzFn =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_tstz_str",
            TypeCreator.REQUIRED.I64,
            ImmutableEnumArg.builder().value("MONTH").build(),
            Expression.TimestampTZLiteral.builder().value(0).build(),
            Expression.StrLiteral.builder().value("GMT").build());

    Project query = b.project(i -> List.of(reqTstzFn), b.emptyScan());

    SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);
    RelNode calciteRel = substraitToCalcite.convert(query);

    RexNode calciteExpr = ((LogicalProject) calciteRel).getProjects().get(0);
    assertEquals(SqlKind.EXTRACT, calciteExpr.getKind());
    assertInstanceOf(RexCall.class, calciteExpr);

    RexCall extract = (RexCall) calciteExpr;
    assertEquals(3, extract.getOperands().size());

    RexNode operand1 = extract.getOperands().get(0);
    assertInstanceOf(RexLiteral.class, operand1);
    assertEquals(SqlKind.LITERAL, operand1.getKind());
    assertEquals(SqlTypeName.SYMBOL, operand1.getType().getSqlTypeName());
    assertEquals(TimeUnitRange.MONTH, ((RexLiteral) operand1).getValueAs(TimeUnitRange.class));

    RexNode operand2 = extract.getOperands().get(1);
    assertInstanceOf(RexLiteral.class, operand2);
    assertEquals(SqlKind.LITERAL, operand2.getKind());
    assertEquals(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, operand2.getType().getSqlTypeName());
    assertEquals(
        0, ((RexLiteral) operand2).getValueAs(TimestampString.class).getMillisSinceEpoch());

    RexNode operand3 = extract.getOperands().get(2);
    assertInstanceOf(RexLiteral.class, operand3);
    assertEquals(SqlKind.LITERAL, operand3.getKind());
    assertEquals(SqlTypeName.VARCHAR, operand3.getType().getSqlTypeName());
    assertEquals("GMT", RexLiteral.stringValue(operand3));
  }

  @Test
  public void extractPrecisionTimestampTzScalarFunction() {
    ScalarFunctionInvocation reqPtstzFn =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_ptstz_str",
            TypeCreator.REQUIRED.I64,
            ImmutableEnumArg.builder().value("MONTH").build(),
            Expression.PrecisionTimestampTZLiteral.builder().value(0).precision(6).build(),
            Expression.StrLiteral.builder().value("GMT").build());

    Project query = b.project(i -> List.of(reqPtstzFn), b.emptyScan());

    SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);
    RelNode calciteRel = substraitToCalcite.convert(query);

    RexNode calciteExpr = ((LogicalProject) calciteRel).getProjects().get(0);
    assertEquals(SqlKind.EXTRACT, calciteExpr.getKind());
    assertInstanceOf(RexCall.class, calciteExpr);

    RexCall extract = (RexCall) calciteExpr;
    assertEquals(3, extract.getOperands().size());

    RexNode operand1 = extract.getOperands().get(0);
    assertInstanceOf(RexLiteral.class, operand1);
    assertEquals(SqlKind.LITERAL, operand1.getKind());
    assertEquals(SqlTypeName.SYMBOL, operand1.getType().getSqlTypeName());
    assertEquals(TimeUnitRange.MONTH, ((RexLiteral) operand1).getValueAs(TimeUnitRange.class));

    RexNode operand2 = extract.getOperands().get(1);
    assertInstanceOf(RexLiteral.class, operand2);
    assertEquals(SqlKind.LITERAL, operand2.getKind());
    assertEquals(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, operand2.getType().getSqlTypeName());
    assertEquals(
        0, ((RexLiteral) operand2).getValueAs(TimestampString.class).getMillisSinceEpoch());

    RexNode operand3 = extract.getOperands().get(2);
    assertInstanceOf(RexLiteral.class, operand3);
    assertEquals(SqlKind.LITERAL, operand3.getKind());
    assertEquals(SqlTypeName.VARCHAR, operand3.getType().getSqlTypeName());
    assertEquals("GMT", RexLiteral.stringValue(operand3));
  }

  @Test
  public void extractTimestampScalarFunction() {
    ScalarFunctionInvocation reqTsFn =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_ts",
            TypeCreator.REQUIRED.I64,
            ImmutableEnumArg.builder().value("MONTH").build(),
            Expression.TimestampLiteral.builder().value(0).build());
    Project query = b.project(i -> List.of(reqTsFn), b.emptyScan());

    SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);
    RelNode calciteRel = substraitToCalcite.convert(query);

    RexNode calciteExpr = ((LogicalProject) calciteRel).getProjects().get(0);
    assertEquals(SqlKind.EXTRACT, calciteExpr.getKind());
    assertInstanceOf(RexCall.class, calciteExpr);

    RexCall extract = (RexCall) calciteExpr;
    assertEquals(2, extract.getOperands().size());

    RexNode operand1 = extract.getOperands().get(0);
    assertInstanceOf(RexLiteral.class, operand1);
    assertEquals(SqlKind.LITERAL, operand1.getKind());
    assertEquals(SqlTypeName.SYMBOL, operand1.getType().getSqlTypeName());
    assertEquals(TimeUnitRange.MONTH, ((RexLiteral) operand1).getValueAs(TimeUnitRange.class));

    RexNode operand2 = extract.getOperands().get(1);
    assertInstanceOf(RexLiteral.class, operand2);
    assertEquals(SqlKind.LITERAL, operand2.getKind());
    assertEquals(SqlTypeName.TIMESTAMP, operand2.getType().getSqlTypeName());
    assertEquals(
        0, ((RexLiteral) operand2).getValueAs(TimestampString.class).getMillisSinceEpoch());
  }

  @Test
  public void extractPrecisionTimestampScalarFunction() {
    ScalarFunctionInvocation reqPtsFn =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_pts",
            TypeCreator.REQUIRED.I64,
            ImmutableEnumArg.builder().value("MONTH").build(),
            Expression.PrecisionTimestampLiteral.builder().value(0).precision(6).build());
    Project query = b.project(i -> List.of(reqPtsFn), b.emptyScan());

    SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);
    RelNode calciteRel = substraitToCalcite.convert(query);

    RexNode calciteExpr = ((LogicalProject) calciteRel).getProjects().get(0);
    assertEquals(SqlKind.EXTRACT, calciteExpr.getKind());
    assertInstanceOf(RexCall.class, calciteExpr);

    RexCall extract = (RexCall) calciteExpr;
    assertEquals(2, extract.getOperands().size());

    RexNode operand1 = extract.getOperands().get(0);
    assertInstanceOf(RexLiteral.class, operand1);
    assertEquals(SqlKind.LITERAL, operand1.getKind());
    assertEquals(SqlTypeName.SYMBOL, operand1.getType().getSqlTypeName());
    assertEquals(TimeUnitRange.MONTH, ((RexLiteral) operand1).getValueAs(TimeUnitRange.class));

    RexNode operand2 = extract.getOperands().get(1);
    assertInstanceOf(RexLiteral.class, operand2);
    assertEquals(SqlKind.LITERAL, operand2.getKind());
    assertEquals(SqlTypeName.TIMESTAMP, operand2.getType().getSqlTypeName());
    assertEquals(
        0, ((RexLiteral) operand2).getValueAs(TimestampString.class).getMillisSinceEpoch());
  }

  @Test
  public void extractDateScalarFunction() {
    ScalarFunctionInvocation reqDateFn =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_date",
            TypeCreator.REQUIRED.I64,
            ImmutableEnumArg.builder().value("MONTH").build(),
            Expression.DateLiteral.builder().value(0).build());
    Project query = b.project(i -> List.of(reqDateFn), b.emptyScan());

    SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);
    RelNode calciteRel = substraitToCalcite.convert(query);

    RexNode calciteExpr = ((LogicalProject) calciteRel).getProjects().get(0);
    assertEquals(SqlKind.EXTRACT, calciteExpr.getKind());
    assertInstanceOf(RexCall.class, calciteExpr);

    RexCall extract = (RexCall) calciteExpr;
    assertEquals(2, extract.getOperands().size());

    RexNode operand1 = extract.getOperands().get(0);
    assertInstanceOf(RexLiteral.class, operand1);
    assertEquals(SqlKind.LITERAL, operand1.getKind());
    assertEquals(SqlTypeName.SYMBOL, operand1.getType().getSqlTypeName());
    assertEquals(TimeUnitRange.MONTH, ((RexLiteral) operand1).getValueAs(TimeUnitRange.class));

    RexNode operand2 = extract.getOperands().get(1);
    assertInstanceOf(RexLiteral.class, operand2);
    assertEquals(SqlKind.LITERAL, operand2.getKind());
    assertEquals(SqlTypeName.DATE, operand2.getType().getSqlTypeName());
    assertEquals(0, ((RexLiteral) operand2).getValueAs(DateString.class).getMillisSinceEpoch());
  }

  @Test
  public void extractTimeScalarFunction() {
    ScalarFunctionInvocation reqTimeFn =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_time",
            TypeCreator.REQUIRED.I64,
            ImmutableEnumArg.builder().value("MINUTE").build(),
            Expression.TimeLiteral.builder().value(0).build());

    Project query = b.project(i -> List.of(reqTimeFn), b.emptyScan());

    SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);
    RelNode calciteRel = substraitToCalcite.convert(query);

    RexNode calciteExpr = ((LogicalProject) calciteRel).getProjects().get(0);
    assertEquals(SqlKind.EXTRACT, calciteExpr.getKind());
    assertInstanceOf(RexCall.class, calciteExpr);

    RexCall extract = (RexCall) calciteExpr;
    assertEquals(2, extract.getOperands().size());

    RexNode operand1 = extract.getOperands().get(0);
    assertInstanceOf(RexLiteral.class, operand1);
    assertEquals(SqlKind.LITERAL, operand1.getKind());
    assertEquals(SqlTypeName.SYMBOL, operand1.getType().getSqlTypeName());
    assertEquals(TimeUnitRange.MINUTE, ((RexLiteral) operand1).getValueAs(TimeUnitRange.class));

    RexNode operand2 = extract.getOperands().get(1);
    assertInstanceOf(RexLiteral.class, operand2);
    assertEquals(SqlKind.LITERAL, operand2.getKind());
    assertEquals(SqlTypeName.TIME, operand2.getType().getSqlTypeName());
    assertEquals(0, ((RexLiteral) operand2).getValueAs(TimeString.class).getMillisOfDay());
  }

  @Test
  public void unsupportedExtractTimestampTzWithIndexing() {
    ScalarFunctionInvocation reqReqTstzFn =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_req_tstz_str",
            TypeCreator.REQUIRED.I64,
            ImmutableEnumArg.builder().value("MONTH").build(),
            ImmutableEnumArg.builder().value("ONE").build(),
            Expression.TimestampTZLiteral.builder().value(0).build(),
            Expression.StrLiteral.builder().value("GMT").build());

    Project query = b.project(i -> List.of(reqReqTstzFn), b.emptyScan());

    SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);
    assertThrows(UnsupportedOperationException.class, () -> substraitToCalcite.convert(query));
  }

  @Test
  public void unsupportedExtractPrecisionTimestampTzWithIndexing() {
    ScalarFunctionInvocation reqReqPtstzFn =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_req_ptstz_str",
            TypeCreator.REQUIRED.I64,
            ImmutableEnumArg.builder().value("MONTH").build(),
            ImmutableEnumArg.builder().value("ONE").build(),
            Expression.PrecisionTimestampTZLiteral.builder().value(0).precision(6).build(),
            Expression.StrLiteral.builder().value("GMT").build());

    Project query = b.project(i -> List.of(reqReqPtstzFn), b.emptyScan());

    SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);
    assertThrows(UnsupportedOperationException.class, () -> substraitToCalcite.convert(query));
  }

  @Test
  public void unsupportedExtractTimestampWithIndexing() {
    ScalarFunctionInvocation reqReqTsFn =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_req_ts",
            TypeCreator.REQUIRED.I64,
            ImmutableEnumArg.builder().value("MONTH").build(),
            ImmutableEnumArg.builder().value("ONE").build(),
            Expression.TimestampLiteral.builder().value(0).build());

    Project query = b.project(i -> List.of(reqReqTsFn), b.emptyScan());

    SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);
    assertThrows(UnsupportedOperationException.class, () -> substraitToCalcite.convert(query));
  }

  @Test
  public void unsupportedExtractPrecisionTimestampWithIndexing() {
    ScalarFunctionInvocation reqReqPtsFn =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_req_pts",
            TypeCreator.REQUIRED.I64,
            ImmutableEnumArg.builder().value("MONTH").build(),
            ImmutableEnumArg.builder().value("ONE").build(),
            Expression.PrecisionTimestampLiteral.builder().value(0).precision(6).build());

    Project query = b.project(i -> List.of(reqReqPtsFn), b.emptyScan());

    SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);
    assertThrows(UnsupportedOperationException.class, () -> substraitToCalcite.convert(query));
  }

  @Test
  public void unsupportedExtractDateWithIndexing() {
    ScalarFunctionInvocation reqReqDateFn =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_req_date",
            TypeCreator.REQUIRED.I64,
            ImmutableEnumArg.builder().value("MONTH").build(),
            ImmutableEnumArg.builder().value("ONE").build(),
            Expression.DateLiteral.builder().value(0).build());

    Project query = b.project(i -> List.of(reqReqDateFn), b.emptyScan());

    SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);
    assertThrows(UnsupportedOperationException.class, () -> substraitToCalcite.convert(query));
  }
}
