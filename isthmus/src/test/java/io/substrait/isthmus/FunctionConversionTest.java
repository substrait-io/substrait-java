package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.EnumArg;
import io.substrait.expression.Expression;
import io.substrait.expression.Expression.ScalarFunctionInvocation;
import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.DefaultExtensionCatalog;
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
public class FunctionConversionTest extends PlanTestBase {

  final SubstraitBuilder b = new SubstraitBuilder(extensions);

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
  public void subtractDateIDay() {
    // When this function is converted to Calcite, if the Calcite type derivation is used an
    // java.lang.ArrayIndexOutOfBoundsException is thrown. It is quite likely that this is being
    // mapped to the wrong
    // Calcite function.
    // TODO: https://github.com/substrait-io/substrait-java/issues/377
    Expression.ScalarFunctionInvocation expr =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "subtract:date_iday",
            TypeCreator.REQUIRED.DATE,
            ExpressionCreator.date(false, 10561),
            ExpressionCreator.intervalDay(false, 120, 0, 0, 6));

    var calciteExpr = expr.accept(expressionRexConverter);
    assertEquals(
        TypeConverter.DEFAULT.toCalcite(typeFactory, TypeCreator.REQUIRED.DATE),
        calciteExpr.getType());

    // TODO: remove once this can be converted back to Substrait
    assertThrows(IllegalArgumentException.class, () -> calciteExpr.accept(rexExpressionConverter));
  }

  @Test
  public void extractTimestampTzScalarFunction() {
    ScalarFunctionInvocation reqTstzFn =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_tstz_str",
            TypeCreator.REQUIRED.I64,
            EnumArg.builder().value("MONTH").build(),
            Expression.TimestampTZLiteral.builder().value(0).build(),
            Expression.StrLiteral.builder().value("GMT").build());

    RexNode calciteExpr = reqTstzFn.accept(expressionRexConverter);
    assertEquals(SqlKind.EXTRACT, calciteExpr.getKind());
    assertInstanceOf(RexCall.class, calciteExpr);

    RexCall extract = (RexCall) calciteExpr;
    assertEquals(
        "EXTRACT(FLAG(MONTH), 1970-01-01 00:00:00:TIMESTAMP_WITH_LOCAL_TIME_ZONE(6), 'GMT':VARCHAR)",
        extract.toString());
  }

  @Test
  public void extractPrecisionTimestampTzScalarFunction() {
    ScalarFunctionInvocation reqPtstzFn =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_ptstz_str",
            TypeCreator.REQUIRED.I64,
            EnumArg.builder().value("MONTH").build(),
            Expression.PrecisionTimestampTZLiteral.builder().value(0).precision(6).build(),
            Expression.StrLiteral.builder().value("GMT").build());

    RexNode calciteExpr = reqPtstzFn.accept(expressionRexConverter);
    assertEquals(SqlKind.EXTRACT, calciteExpr.getKind());
    assertInstanceOf(RexCall.class, calciteExpr);

    RexCall extract = (RexCall) calciteExpr;
    assertEquals(
        "EXTRACT(FLAG(MONTH), 1970-01-01 00:00:00:TIMESTAMP_WITH_LOCAL_TIME_ZONE(6), 'GMT':VARCHAR)",
        extract.toString());
  }

  @Test
  public void extractTimestampScalarFunction() {
    ScalarFunctionInvocation reqTsFn =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_ts",
            TypeCreator.REQUIRED.I64,
            EnumArg.builder().value("MONTH").build(),
            Expression.TimestampLiteral.builder().value(0).build());

    RexNode calciteExpr = reqTsFn.accept(expressionRexConverter);
    assertEquals(SqlKind.EXTRACT, calciteExpr.getKind());
    assertInstanceOf(RexCall.class, calciteExpr);

    RexCall extract = (RexCall) calciteExpr;
    assertEquals("EXTRACT(FLAG(MONTH), 1970-01-01 00:00:00:TIMESTAMP(6))", extract.toString());
  }

  @Test
  public void extractPrecisionTimestampScalarFunction() {
    ScalarFunctionInvocation reqPtsFn =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_pts",
            TypeCreator.REQUIRED.I64,
            EnumArg.builder().value("MONTH").build(),
            Expression.PrecisionTimestampLiteral.builder().value(0).precision(6).build());

    RexNode calciteExpr = reqPtsFn.accept(expressionRexConverter);
    assertEquals(SqlKind.EXTRACT, calciteExpr.getKind());
    assertInstanceOf(RexCall.class, calciteExpr);

    RexCall extract = (RexCall) calciteExpr;
    assertEquals("EXTRACT(FLAG(MONTH), 1970-01-01 00:00:00:TIMESTAMP(6))", extract.toString());
  }

  @Test
  public void extractDateScalarFunction() {
    ScalarFunctionInvocation reqDateFn =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_date",
            TypeCreator.REQUIRED.I64,
            EnumArg.builder().value("MONTH").build(),
            Expression.DateLiteral.builder().value(0).build());

    RexNode calciteExpr = reqDateFn.accept(expressionRexConverter);
    assertEquals(SqlKind.EXTRACT, calciteExpr.getKind());
    assertInstanceOf(RexCall.class, calciteExpr);

    RexCall extract = (RexCall) calciteExpr;
    assertEquals("EXTRACT(FLAG(MONTH), 1970-01-01)", extract.toString());
  }

  @Test
  public void extractTimeScalarFunction() {
    ScalarFunctionInvocation reqTimeFn =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_time",
            TypeCreator.REQUIRED.I64,
            EnumArg.builder().value("MINUTE").build(),
            Expression.TimeLiteral.builder().value(0).build());

    RexNode calciteExpr = reqTimeFn.accept(expressionRexConverter);
    assertEquals(SqlKind.EXTRACT, calciteExpr.getKind());
    assertInstanceOf(RexCall.class, calciteExpr);

    RexCall extract = (RexCall) calciteExpr;
    assertEquals("EXTRACT(FLAG(MINUTE), 00:00:00:TIME(6))", extract.toString());
  }

  @Test
  public void unsupportedExtractTimestampTzWithIndexing() {
    ScalarFunctionInvocation reqReqTstzFn =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_req_tstz_str",
            TypeCreator.REQUIRED.I64,
            EnumArg.builder().value("MONTH").build(),
            EnumArg.builder().value("ONE").build(),
            Expression.TimestampTZLiteral.builder().value(0).build(),
            Expression.StrLiteral.builder().value("GMT").build());

    assertThrows(
        UnsupportedOperationException.class, () -> reqReqTstzFn.accept(expressionRexConverter));
  }

  @Test
  public void unsupportedExtractPrecisionTimestampTzWithIndexing() {
    ScalarFunctionInvocation reqReqPtstzFn =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_req_ptstz_str",
            TypeCreator.REQUIRED.I64,
            EnumArg.builder().value("MONTH").build(),
            EnumArg.builder().value("ONE").build(),
            Expression.PrecisionTimestampTZLiteral.builder().value(0).precision(6).build(),
            Expression.StrLiteral.builder().value("GMT").build());

    assertThrows(
        UnsupportedOperationException.class, () -> reqReqPtstzFn.accept(expressionRexConverter));
  }

  @Test
  public void unsupportedExtractTimestampWithIndexing() {
    ScalarFunctionInvocation reqReqTsFn =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_req_ts",
            TypeCreator.REQUIRED.I64,
            EnumArg.builder().value("MONTH").build(),
            EnumArg.builder().value("ONE").build(),
            Expression.TimestampLiteral.builder().value(0).build());

    assertThrows(
        UnsupportedOperationException.class, () -> reqReqTsFn.accept(expressionRexConverter));
  }

  @Test
  public void unsupportedExtractPrecisionTimestampWithIndexing() {
    ScalarFunctionInvocation reqReqPtsFn =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_req_pts",
            TypeCreator.REQUIRED.I64,
            EnumArg.builder().value("MONTH").build(),
            EnumArg.builder().value("ONE").build(),
            Expression.PrecisionTimestampLiteral.builder().value(0).precision(6).build());

    assertThrows(
        UnsupportedOperationException.class, () -> reqReqPtsFn.accept(expressionRexConverter));
  }

  @Test
  public void unsupportedExtractDateWithIndexing() {
    ScalarFunctionInvocation reqReqDateFn =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_DATETIME,
            "extract:req_req_date",
            TypeCreator.REQUIRED.I64,
            EnumArg.builder().value("MONTH").build(),
            EnumArg.builder().value("ONE").build(),
            Expression.DateLiteral.builder().value(0).build());

    assertThrows(
        UnsupportedOperationException.class, () -> reqReqDateFn.accept(expressionRexConverter));
  }

  @Test
  public void concatStringLiteralAndVarchar() throws Exception {
    assertProtoPlanRoundrip("select 'part_'||P_NAME from PART");
  }

  @Test
  public void concatCharAndVarchar() throws Exception {
    assertProtoPlanRoundrip("select P_BRAND||P_NAME from PART");
  }

  @Test
  public void concatStringLiteralAndChar() throws Exception {
    assertProtoPlanRoundrip("select 'brand_'||P_BRAND from PART");
  }
}
