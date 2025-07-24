package io.substrait.examples.util;

import io.substrait.expression.EnumArg;
import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg.FuncArgVisitor;
import io.substrait.extension.SimpleExtension.Function;
import io.substrait.type.Type;
import io.substrait.util.EmptyVisitationContext;

/** FunctionArgStringify produces a simple debug string for Function Arguments */
public class FunctionArgStringify extends ParentStringify
    implements FuncArgVisitor<String, EmptyVisitationContext, RuntimeException> {

  public FunctionArgStringify(int indent) {
    super(indent);
  }

  @Override
  public String visitExpr(Function fnDef, int argIdx, Expression e, EmptyVisitationContext context)
      throws RuntimeException {
    return e.accept(new ExpressionStringify(indent + 1), context);
  }

  @Override
  public String visitType(Function fnDef, int argIdx, Type t, EmptyVisitationContext context)
      throws RuntimeException {
    return t.accept(new TypeStringify(indent));
  }

  @Override
  public String visitEnumArg(Function fnDef, int argIdx, EnumArg e, EmptyVisitationContext context)
      throws RuntimeException {
    return e.toString();
  }
}
