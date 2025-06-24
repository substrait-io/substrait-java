package io.substrait.isthmus.expression;

import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import java.util.List;
import org.apache.calcite.rex.RexNode;

/**
 * Associates operands with a matching Substrait function and possible matching function
 * implementations.
 */
class SubstraitFunctionMapping {

  private final String substraitName;
  private final List<RexNode> operands;
  private final List<ScalarFunctionVariant> functions;

  public SubstraitFunctionMapping(
      final String substraitName,
      final List<RexNode> operands,
      final List<ScalarFunctionVariant> functions) {
    this.substraitName = substraitName;
    this.operands = operands;
    this.functions = functions;
  }

  String substraitName() {
    return substraitName;
  }

  List<RexNode> operands() {
    return operands;
  }

  List<ScalarFunctionVariant> functions() {
    return functions;
  }
}
