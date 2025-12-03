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

  public FunctionArgStringify(final int indent) {
    super(indent);
  }

  @Override
  public String visitExpr(
      final Function fnDef,
      final int argIdx,
      final Expression e,
      final EmptyVisitationContext context)
      throws RuntimeException {
    return e.accept(new ExpressionStringify(indent + 1), context);
  }

  @Override
  public String visitType(
      final Function fnDef, final int argIdx, final Type t, final EmptyVisitationContext context)
      throws RuntimeException {
    return t.accept(new TypeStringify(indent));
  }

  @Override
  public String visitEnumArg(
      final Function fnDef, final int argIdx, final EnumArg e, final EmptyVisitationContext context)
      throws RuntimeException {
    return e.toString();
  }
}
