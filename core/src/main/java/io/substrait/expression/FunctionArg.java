package io.substrait.expression;

import io.substrait.expression.proto.ProtoExpressionConverter;
import io.substrait.function.SimpleExtension;
import io.substrait.function.TypeExpressionVisitor;
import io.substrait.proto.FunctionArgument;
import io.substrait.type.Type;
import io.substrait.type.proto.FromProto;

/**
 * FunctionArg is a marker interface that represents an argument of a {@link
 * SimpleExtension.Function} invocation. Subtypes are {@link Expression}, {@link Type}, and {@link
 * EnumArg}. Methods processing/visiting FunctionArg instances should be passed the {@link
 * SimpleExtension.Function} and the <i>argument index</i>.
 */
public interface FunctionArg {

  <R, E extends Throwable> R accept(
      SimpleExtension.Function fnDef, int argIdx, FuncArgVisitor<R, E> fnArgVisitor) throws E;

  interface FuncArgVisitor<R, E extends Throwable> {
    R visitExpr(SimpleExtension.Function fnDef, int argIdx, Expression e) throws E;

    R visitType(SimpleExtension.Function fnDef, int argIdx, Type t) throws E;

    R visitEnumArg(SimpleExtension.Function fnDef, int argIdx, EnumArg e) throws E;
  }

  static FuncArgVisitor<FunctionArgument, RuntimeException> toProto(
      TypeExpressionVisitor<io.substrait.proto.Type, RuntimeException> typeVisitor,
      ExpressionVisitor<io.substrait.proto.Expression, RuntimeException> expressionVisitor) {

    return new FuncArgVisitor<FunctionArgument, RuntimeException>() {

      @Override
      public FunctionArgument visitExpr(SimpleExtension.Function fnDef, int argIdx, Expression e)
          throws RuntimeException {
        return FunctionArgument.newBuilder().setValue(e.accept(expressionVisitor)).build();
      }

      @Override
      public FunctionArgument visitType(SimpleExtension.Function fnDef, int argIdx, Type t)
          throws RuntimeException {
        return FunctionArgument.newBuilder().setType(t.accept(typeVisitor)).build();
      }

      @Override
      public FunctionArgument visitEnumArg(SimpleExtension.Function fnDef, int argIdx, EnumArg ea)
          throws RuntimeException {
        FunctionArgument.Enum.Builder enumBldr = FunctionArgument.Enum.newBuilder();

        if (ea.value().isPresent()) {
          enumBldr = enumBldr.setSpecified(ea.value().get());
        }
        return FunctionArgument.newBuilder().setEnum(enumBldr.build()).build();
      }
    };
  }

  class ProtoFrom {
    private final ProtoExpressionConverter exprBuilder;

    public ProtoFrom(ProtoExpressionConverter exprBuilder) {
      this.exprBuilder = exprBuilder;
    }

    public FunctionArg convert(
        SimpleExtension.Function funcDef, int argIdx, FunctionArgument fArg) {

      switch (fArg.getArgTypeCase()) {
        case TYPE:
          return FromProto.from(fArg.getType());
        case VALUE:
          return exprBuilder.from(fArg.getValue());
        case ENUM:
          {
            SimpleExtension.EnumArgument enumArgDef =
                (SimpleExtension.EnumArgument) funcDef.args().get(argIdx);
            String optionValue = fArg.getEnum().getSpecified();
            return optionValue == null
                ? EnumArg.UNSPECIFIED_ENUM_ARG
                : EnumArg.of(enumArgDef, optionValue);
          }
        default:
          throw new UnsupportedOperationException(
              String.format("Unable to convert FunctionArgument %s.", fArg));
      }
    }
  }
}
