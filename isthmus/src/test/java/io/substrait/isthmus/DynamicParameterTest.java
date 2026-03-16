package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.substrait.expression.Expression;
import io.substrait.isthmus.SubstraitRelNodeConverter.Context;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.Rel.Remap;
import io.substrait.type.Type;
import java.util.List;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.junit.jupiter.api.Test;

class DynamicParameterTest extends PlanTestBase {

  final List<Type> commonTableType = List.of(R.I32, R.FP32, N.STRING, N.BOOLEAN);

  final ExpressionRexConverter expressionRexConverter =
      new ExpressionRexConverter(
          typeFactory,
          new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory),
          new WindowFunctionConverter(extensions.windowFunctions(), typeFactory),
          TypeConverter.DEFAULT);

  @Test
  void dynamicParameterToCalcite() {
    Expression.DynamicParameter dp =
        Expression.DynamicParameter.builder().type(R.I64).parameterReference(0).build();

    RexNode calciteExpr = dp.accept(expressionRexConverter, Context.newContext());

    assertInstanceOf(RexDynamicParam.class, calciteExpr);
    RexDynamicParam rexDp = (RexDynamicParam) calciteExpr;
    assertEquals(0, rexDp.getIndex());
    assertEquals(TypeConverter.DEFAULT.toCalcite(typeFactory, R.I64), rexDp.getType());
  }

  @Test
  void dynamicParameterNullableStringToCalcite() {
    Expression.DynamicParameter dp =
        Expression.DynamicParameter.builder().type(N.STRING).parameterReference(1).build();

    RexNode calciteExpr = dp.accept(expressionRexConverter, Context.newContext());

    assertInstanceOf(RexDynamicParam.class, calciteExpr);
    RexDynamicParam rexDp = (RexDynamicParam) calciteExpr;
    assertEquals(1, rexDp.getIndex());
    assertEquals(TypeConverter.DEFAULT.toCalcite(typeFactory, N.STRING), rexDp.getType());
  }

  @Test
  void dynamicParameterFP64ToCalcite() {
    Expression.DynamicParameter dp =
        Expression.DynamicParameter.builder().type(R.FP64).parameterReference(5).build();

    RexNode calciteExpr = dp.accept(expressionRexConverter, Context.newContext());

    assertInstanceOf(RexDynamicParam.class, calciteExpr);
    RexDynamicParam rexDp = (RexDynamicParam) calciteExpr;
    assertEquals(5, rexDp.getIndex());
    assertEquals(TypeConverter.DEFAULT.toCalcite(typeFactory, R.FP64), rexDp.getType());
  }

  @Test
  void dynamicParameterInProjectRoundTrip() {
    Rel commonTable =
        sb.namedScan(List.of("example"), List.of("a", "b", "c", "d"), commonTableType);

    Expression.DynamicParameter dp =
        Expression.DynamicParameter.builder().type(R.I32).parameterReference(0).build();

    Project project = sb.project(input -> List.of(dp), Remap.of(List.of(4)), commonTable);
    assertFullRoundTrip(project);
  }

  @Test
  void dynamicParameterMultipleInProjectRoundTrip() {
    Rel commonTable =
        sb.namedScan(List.of("example"), List.of("a", "b", "c", "d"), commonTableType);

    Expression.DynamicParameter dp0 =
        Expression.DynamicParameter.builder().type(R.I32).parameterReference(0).build();

    Expression.DynamicParameter dp1 =
        Expression.DynamicParameter.builder().type(N.STRING).parameterReference(1).build();

    Project project = sb.project(input -> List.of(dp0, dp1), Remap.of(List.of(4, 5)), commonTable);
    assertFullRoundTrip(project);
  }
}
