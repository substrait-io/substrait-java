package io.substrait.examples.util;

import io.substrait.expression.EnumArg;
import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg.FuncArgVisitor;
import io.substrait.extension.SimpleExtension.Function;
import io.substrait.type.Type;

/** FunctionArgStrngify produces a simple debug string for Funcation Arguments */
public class FunctionArgStringify extends ParentStringify
    implements FuncArgVisitor<String, RuntimeException> {

  public FunctionArgStringify(int indent) {
    super(indent);
  }

  @Override
  public String visitExpr(Function fnDef, int argIdx, Expression e) throws RuntimeException {
    return e.accept(new ExpressionStringify(indent + 1));
  }

  @Override
  public String visitType(Function fnDef, int argIdx, Type t) throws RuntimeException {
    return t.accept(new TypeStringify(indent));
  }

  @Override
  public String visitEnumArg(Function fnDef, int argIdx, EnumArg e) throws RuntimeException {
    return e.toString();
  }
}
