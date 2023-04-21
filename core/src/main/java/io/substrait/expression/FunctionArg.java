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

    return new FuncArgVisitor<>() {

      @Override
      public FunctionArgument visitExpr(SimpleExtension.Function fnDef, int argIdx, Expression e)
          throws RuntimeException {
        var pE = e.accept(expressionVisitor);
        return FunctionArgument.newBuilder().setValue(pE).build();
      }

      @Override
      public FunctionArgument visitType(SimpleExtension.Function fnDef, int argIdx, Type t)
          throws RuntimeException {
        var pTyp = t.accept(typeVisitor);
        return FunctionArgument.newBuilder().setType(pTyp).build();
      }

      @Override
      public FunctionArgument visitEnumArg(SimpleExtension.Function fnDef, int argIdx, EnumArg ea)
          throws RuntimeException {
        var enumBldr = FunctionArgument.newBuilder();

        if (ea.value().isPresent()) {
          enumBldr = enumBldr.setEnum(ea.value().get());
        }
        return enumBldr.build();
      }
    };
  }

  /**
   * Converts from {@link io.substrait.proto.FunctionArgument} to {@link
   * io.substrait.expression.FunctionArg}
   */
  class ProtoFrom {
    private final ProtoExpressionConverter protoExprConverter;
    private final FromProto protoTypeConverter;

    public ProtoFrom(ProtoExpressionConverter protoExprConverter, FromProto protoTypeConverter) {
      this.protoExprConverter = protoExprConverter;
      this.protoTypeConverter = protoTypeConverter;
    }

    public FunctionArg convert(
        SimpleExtension.Function funcDef, int argIdx, FunctionArgument fArg) {
      return switch (fArg.getArgTypeCase()) {
        case TYPE -> protoTypeConverter.from(fArg.getType());
        case VALUE -> protoExprConverter.from(fArg.getValue());
        case ENUM -> {
          SimpleExtension.EnumArgument enumArgDef =
              (SimpleExtension.EnumArgument) funcDef.args().get(argIdx);
          var optionValue = fArg.getEnum();
          yield EnumArg.of(enumArgDef, optionValue);
        }
        default -> throw new UnsupportedOperationException(
            String.format("Unable to convert FunctionArgument %s.", fArg));
      };
    }
  }
}
