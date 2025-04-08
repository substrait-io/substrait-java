package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.isthmus.expression.CallConverters;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.type.TypeCreator;
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
}
