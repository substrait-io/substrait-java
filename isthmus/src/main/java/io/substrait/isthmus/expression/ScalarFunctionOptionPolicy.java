package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.FunctionOption;
import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import java.util.List;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlOperator;

/** Option semantics for one family of scalar functions, in both conversion directions. */
interface ScalarFunctionOptionPolicy {
  SqlOperator resolve(Expression.ScalarFunctionInvocation expression, SqlOperator operator);

  List<FunctionOption> forCall(RexCall call, ScalarFunctionVariant function);

  /** Returns the operator whose signature also binds this call, such as an unchecked equivalent. */
  default SqlOperator signatureOperator(RexCall call) {
    return call.getOperator();
  }
}
