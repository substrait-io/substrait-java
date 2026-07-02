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
import java.util.stream.Stream;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.junit.jupiter.api.Test;

/**
 * Verify that "problematic" Substrait functions can be converted to Calcite and back successfully
 */
class FunctionConversionTest extends PlanTestBase {
  final ScalarFunctionConverter scalarFnConverter =
      new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory);

  final WindowFunctionConverter windowFnConverter =
      new WindowFunctionConverter(extensions.windowFunctions(), typeFactory);

  final ExpressionRexConverter expressionRexConverter =
      new ExpressionRexConverter(
          typeFactory, scalarFnConverter, windowFnConverter, TypeConverter.DEFAULT);

  final RexExpressionConverter rexExpressionConverter =
      new RexExpressionConverter(
          // a SubstraitRelVisitor is not needed for these tests
          null,
          Stream.concat(
                  CallConverters.defaults(TypeConverter.DEFAULT).stream(),
                  Stream.of(scalarFnConverter))
              .toList(),
          windowFnConverter,
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

    Expression reverse = calciteExpr.accept(rexExpressionConverter);
    assertEquals(expr, reverse);
  }

  @Test
  void extractPrecisionTimestampTzScalarFunction() {
    ScalarFunctionInvocation reqPtstzFn =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_ptstz_str",
            TypeCreator.REQUIRED.I64,
            EnumArg.builder().value("MONTH").build(),
            Expression.PrecisionTimestampTZLiteral.builder().value(0).precision(3).build(),
            Expression.StrLiteral.builder().value("GMT").build());

    RexNode calciteExpr = reqPtstzFn.accept(expressionRexConverter, Context.newContext());
    assertEquals(SqlKind.EXTRACT, calciteExpr.getKind());
    assertInstanceOf(RexCall.class, calciteExpr);

    RexCall extract = (RexCall) calciteExpr;
    assertEquals(
        "EXTRACT(FLAG(MONTH), 1970-01-01 00:00:00:TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), 'GMT':VARCHAR)",
        extract.toString());
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
            "extract:req_pt",
            TypeCreator.REQUIRED.I64,
            EnumArg.builder().value("MINUTE").build(),
            Expression.PrecisionTimeLiteral.builder().value(0).precision(6).build());

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

  @Test
  void strptimeTime() {
    Expression.StrLiteral inputString = Expression.StrLiteral.builder().value("12:34:56").build();
    Expression.StrLiteral formatString = Expression.StrLiteral.builder().value("%H:%M:%S").build();
    Expression.I8Literal precision = ExpressionCreator.i8(false, (byte) 6);
    ScalarFunctionInvocation strptimeFn =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "strptime_time:str_str_i8",
            TypeCreator.REQUIRED.precisionTime(6),
            inputString,
            formatString,
            precision);

    // tests Substrait -> Calcite
    RexNode calciteExpr = strptimeFn.accept(expressionRexConverter, Context.newContext());
    assertEquals(SqlKind.OTHER_FUNCTION, calciteExpr.getKind());
    assertInstanceOf(RexCall.class, calciteExpr);
    assertEquals(
        "PARSE_TIME('%H:%M:%S':VARCHAR, '12:34:56':VARCHAR, 6:TINYINT)", calciteExpr.toString());
  }

  @Test
  void strptimeTimestamp() {
    Expression.StrLiteral inputString =
        Expression.StrLiteral.builder().value("2026-01-29T12:34:56").build();
    Expression.StrLiteral formatString =
        Expression.StrLiteral.builder().value("%Y:%m:%dT%H:%M:%S").build();
    Expression.I8Literal precision = ExpressionCreator.i8(false, (byte) 6);
    ScalarFunctionInvocation strptimeFn =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "strptime_timestamp:str_str_i8",
            TypeCreator.REQUIRED.precisionTimestamp(6),
            inputString,
            formatString,
            precision);

    // tests Substrait -> Calcite
    RexNode calciteExpr = strptimeFn.accept(expressionRexConverter, Context.newContext());
    assertEquals(SqlKind.OTHER_FUNCTION, calciteExpr.getKind());
    assertInstanceOf(RexCall.class, calciteExpr);
    assertEquals(
        "PARSE_TIMESTAMP('%Y:%m:%dT%H:%M:%S':VARCHAR, '2026-01-29T12:34:56':VARCHAR, 6:TINYINT)",
        calciteExpr.toString());
  }

  @Test
  void strptimeDate() {
    Expression.StrLiteral inputString = Expression.StrLiteral.builder().value("2026-01-29").build();
    Expression.StrLiteral formatString = Expression.StrLiteral.builder().value("%Y:%m:%d").build();
    ScalarFunctionInvocation strptimeFn =
        sb.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "strptime_date:str_str",
            TypeCreator.REQUIRED.DATE,
            inputString,
            formatString);

    // tests Substrait -> Calcite
    RexNode calciteExpr = strptimeFn.accept(expressionRexConverter, Context.newContext());
    assertEquals(SqlKind.OTHER_FUNCTION, calciteExpr.getKind());
    assertInstanceOf(RexCall.class, calciteExpr);
    assertEquals("PARSE_DATE('%Y:%m:%d':VARCHAR, '2026-01-29':VARCHAR)", calciteExpr.toString());

    // tests the reverse Calcite -> Substrait
    Expression reverse = calciteExpr.accept(rexExpressionConverter);
    assertEquals(strptimeFn, reverse);
  }
}
