package io.substrait.isthmus.expression;

import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import java.util.List;
import org.apache.calcite.rex.RexNode;

class SubstraitFunctionMapping {

  private final String name;
  private final List<RexNode> operands;
  private final List<ScalarFunctionVariant> functions;

  public SubstraitFunctionMapping(
      final String name,
      final List<RexNode> operands,
      final List<ScalarFunctionVariant> functions) {
    this.name = name;
    this.operands = operands;
    this.functions = functions;
  }

  String name() {
    return name;
  }

  List<RexNode> operands() {
    return operands;
  }

  List<ScalarFunctionVariant> functions() {
    return functions;
  }
}
