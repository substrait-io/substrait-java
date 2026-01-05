package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.expression.EnumArg;
import io.substrait.expression.Expression;
import io.substrait.expression.Expression.ScalarFunctionInvocation;
import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.isthmus.SubstraitRelNodeConverter.Context;
import io.substrait.isthmus.expression.CallConverters;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.type.TypeCreator;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.junit.jupiter.api.Test;

/**
 * Verify that "problematic" Substrait functions can be converted to Calcite and back successfully
 */
class FunctionConversionTest extends PlanTestBase {

  final ExpressionRexConverter expressionRexConverter =
      new ExpressionRexConverter(
          typeFactory,
          new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory),
          new WindowFunctionConverter(extensions.windowFunctions(), typeFactory),
          TypeConverter.DEFAULT);

  final RexExpressionConverter rexExpressionConverter =
      new RexExpressionConverter(
          // a SubstraitRelVisitor is not needed for these tests
          null,
          CallConverters.defaults(TypeConverter.DEFAULT),
          // TODO: set WindowFunctionConverter if/when tests for window functions are added
          null,
          TypeConverter.DEFAULT);

  @Test
  void subtractDateIDay() {
    // When this function is converted to Calcite, if the Calcite type derivation is used an
    // java.lang.ArrayIndexOutOfBoundsException is thrown. It is quite likely that
    // this is being mapped to the wrong Calcite function.
    // TODO: https://github.com/substrait-io/substrait-java/issues/377
    Expression.ScalarFunctionInvocation expr =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "subtract:date_iday",
            TypeCreator.REQUIRED.DATE,
            ExpressionCreator.date(false, 10561),
            ExpressionCreator.intervalDay(false, 120, 0, 0, 6));

    RexNode calciteExpr = expr.accept(expressionRexConverter, Context.newContext());
    assertEquals(
        TypeConverter.DEFAULT.toCalcite(typeFactory, TypeCreator.REQUIRED.DATE),
        calciteExpr.getType());

    // TODO: remove once this can be converted back to Substrait
    assertThrows(IllegalArgumentException.class, () -> calciteExpr.accept(rexExpressionConverter));
  }

  @Test
  void extractTimestampTzScalarFunction() {
    ScalarFunctionInvocation reqTstzFn =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_tstz_str",
            TypeCreator.REQUIRED.I64,
            EnumArg.builder().value("MONTH").build(),
            Expression.TimestampTZLiteral.builder().value(0).build(),
            Expression.StrLiteral.builder().value("GMT").build());

    RexNode calciteExpr = reqTstzFn.accept(expressionRexConverter, Context.newContext());
    assertEquals(SqlKind.EXTRACT, calciteExpr.getKind());
    assertInstanceOf(RexCall.class, calciteExpr);

    RexCall extract = (RexCall) calciteExpr;
    assertEquals(
        "EXTRACT(FLAG(MONTH), 1970-01-01 00:00:00:TIMESTAMP_WITH_LOCAL_TIME_ZONE(6), 'GMT':VARCHAR)",
        extract.toString());
  }

  @Test
  void extractPrecisionTimestampTzScalarFunction() {
    ScalarFunctionInvocation reqPtstzFn =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_ptstz_str",
            TypeCreator.REQUIRED.I64,
            EnumArg.builder().value("MONTH").build(),
            Expression.PrecisionTimestampTZLiteral.builder().value(0).precision(6).build(),
            Expression.StrLiteral.builder().value("GMT").build());

    RexNode calciteExpr = reqPtstzFn.accept(expressionRexConverter, Context.newContext());
    assertEquals(SqlKind.EXTRACT, calciteExpr.getKind());
    assertInstanceOf(RexCall.class, calciteExpr);

    RexCall extract = (RexCall) calciteExpr;
    assertEquals(
        "EXTRACT(FLAG(MONTH), 1970-01-01 00:00:00:TIMESTAMP_WITH_LOCAL_TIME_ZONE(6), 'GMT':VARCHAR)",
        extract.toString());
  }

  @Test
  void extractTimestampScalarFunction() {
    ScalarFunctionInvocation reqTsFn =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_ts",
            TypeCreator.REQUIRED.I64,
            EnumArg.builder().value("MONTH").build(),
            Expression.TimestampLiteral.builder().value(0).build());

    RexNode calciteExpr = reqTsFn.accept(expressionRexConverter, Context.newContext());
    assertEquals(SqlKind.EXTRACT, calciteExpr.getKind());
    assertInstanceOf(RexCall.class, calciteExpr);

    RexCall extract = (RexCall) calciteExpr;
    assertEquals("EXTRACT(FLAG(MONTH), 1970-01-01 00:00:00:TIMESTAMP(6))", extract.toString());
  }

  @Test
  void extractPrecisionTimestampScalarFunction() {
    ScalarFunctionInvocation reqPtsFn =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_pts",
            TypeCreator.REQUIRED.I64,
            EnumArg.builder().value("MONTH").build(),
            Expression.PrecisionTimestampLiteral.builder().value(0).precision(6).build());

    RexNode calciteExpr = reqPtsFn.accept(expressionRexConverter, Context.newContext());
    assertEquals(SqlKind.EXTRACT, calciteExpr.getKind());
    assertInstanceOf(RexCall.class, calciteExpr);

    RexCall extract = (RexCall) calciteExpr;
    assertEquals("EXTRACT(FLAG(MONTH), 1970-01-01 00:00:00:TIMESTAMP(6))", extract.toString());
  }

  @Test
  void extractDateScalarFunction() {
    ScalarFunctionInvocation reqDateFn =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_date",
            TypeCreator.REQUIRED.I64,
            EnumArg.builder().value("MONTH").build(),
            Expression.DateLiteral.builder().value(0).build());

    RexNode calciteExpr = reqDateFn.accept(expressionRexConverter, Context.newContext());
    assertEquals(SqlKind.EXTRACT, calciteExpr.getKind());
    assertInstanceOf(RexCall.class, calciteExpr);

    RexCall extract = (RexCall) calciteExpr;
    assertEquals("EXTRACT(FLAG(MONTH), 1970-01-01)", extract.toString());
  }

  @Test
  void extractTimeScalarFunction() {
    ScalarFunctionInvocation reqTimeFn =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_time",
            TypeCreator.REQUIRED.I64,
            EnumArg.builder().value("MINUTE").build(),
            Expression.TimeLiteral.builder().value(0).build());

    RexNode calciteExpr = reqTimeFn.accept(expressionRexConverter, Context.newContext());
    assertEquals(SqlKind.EXTRACT, calciteExpr.getKind());
    assertInstanceOf(RexCall.class, calciteExpr);

    RexCall extract = (RexCall) calciteExpr;
    assertEquals("EXTRACT(FLAG(MINUTE), 00:00:00:TIME(6))", extract.toString());
  }

  @Test
  void extractDateWithIndexing() {
    ScalarFunctionInvocation reqReqDateFn =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_req_date",
            TypeCreator.REQUIRED.I64,
            EnumArg.builder().value("MONTH").build(),
            EnumArg.builder().value("ONE").build(),
            Expression.DateLiteral.builder().value(0).build());

    RexNode calciteExpr = reqReqDateFn.accept(expressionRexConverter, Context.newContext());
    assertEquals(SqlKind.EXTRACT, calciteExpr.getKind());
    assertInstanceOf(RexCall.class, calciteExpr);

    RexCall extract = (RexCall) calciteExpr;
    assertEquals("EXTRACT(FLAG(MONTH), 1970-01-01)", extract.toString());
  }

  @Test
  void unsupportedExtractTimestampTzWithIndexing() {
    ScalarFunctionInvocation reqReqTstzFn =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_req_tstz_str",
            TypeCreator.REQUIRED.I64,
            EnumArg.builder().value("MONTH").build(),
            EnumArg.builder().value("ONE").build(),
            Expression.TimestampTZLiteral.builder().value(0).build(),
            Expression.StrLiteral.builder().value("GMT").build());

    assertThrows(
        UnsupportedOperationException.class,
        () -> reqReqTstzFn.accept(expressionRexConverter, Context.newContext()));
  }

  @Test
  void unsupportedExtractPrecisionTimestampTzWithIndexing() {
    ScalarFunctionInvocation reqReqPtstzFn =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_req_ptstz_str",
            TypeCreator.REQUIRED.I64,
            EnumArg.builder().value("MONTH").build(),
            EnumArg.builder().value("ONE").build(),
            Expression.PrecisionTimestampTZLiteral.builder().value(0).precision(6).build(),
            Expression.StrLiteral.builder().value("GMT").build());

    assertThrows(
        UnsupportedOperationException.class,
        () -> reqReqPtstzFn.accept(expressionRexConverter, Context.newContext()));
  }

  @Test
  void unsupportedExtractTimestampWithIndexing() {
    ScalarFunctionInvocation reqReqTsFn =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_req_ts",
            TypeCreator.REQUIRED.I64,
            EnumArg.builder().value("MONTH").build(),
            EnumArg.builder().value("ONE").build(),
            Expression.TimestampLiteral.builder().value(0).build());

    assertThrows(
        UnsupportedOperationException.class,
        () -> reqReqTsFn.accept(expressionRexConverter, Context.newContext()));
  }

  @Test
  void unsupportedExtractPrecisionTimestampWithIndexing() {
    ScalarFunctionInvocation reqReqPtsFn =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_req_pts",
            TypeCreator.REQUIRED.I64,
            EnumArg.builder().value("MONTH").build(),
            EnumArg.builder().value("ONE").build(),
            Expression.PrecisionTimestampLiteral.builder().value(0).precision(6).build());

    assertThrows(
        UnsupportedOperationException.class,
        () -> reqReqPtsFn.accept(expressionRexConverter, Context.newContext()));
  }

  @Test
  void concatStringLiteralAndVarchar() throws Exception {
    assertProtoPlanRoundrip("select 'part_'||P_NAME from PART");
  }

  @Test
  void concatCharAndVarchar() throws Exception {
    assertProtoPlanRoundrip("select P_BRAND||P_NAME from PART");
  }

  @Test
  void concatStringLiteralAndChar() throws Exception {
    assertProtoPlanRoundrip("select 'brand_'||P_BRAND from PART");
  }
}
