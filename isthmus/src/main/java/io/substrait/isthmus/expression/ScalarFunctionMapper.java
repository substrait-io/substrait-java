package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rex.RexCall;

interface ScalarFunctionMapper {
  Optional<SubstraitFunctionMapping> toSubstrait(RexCall call);

  Optional<List<FunctionArg>> getRexArguments(Expression.ScalarFunctionInvocation expression);
}
