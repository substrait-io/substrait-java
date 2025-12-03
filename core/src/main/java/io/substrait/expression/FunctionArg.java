package io.substrait.expression;

import io.substrait.expression.proto.ProtoExpressionConverter;
import io.substrait.extension.SimpleExtension;
import io.substrait.function.TypeExpressionVisitor;
import io.substrait.proto.FunctionArgument;
import io.substrait.type.Type;
import io.substrait.type.proto.ProtoTypeConverter;
import io.substrait.util.EmptyVisitationContext;
import io.substrait.util.VisitationContext;

/**
 * FunctionArg is a marker interface that represents an argument of a {@link
 * SimpleExtension.Function} invocation. Subtypes are {@link Expression}, {@link Type}, and {@link
 * EnumArg}. Methods processing/visiting FunctionArg instances should be passed the {@link
 * SimpleExtension.Function} and the <i>argument index</i>.
 */
public interface FunctionArg {

  <R, C extends VisitationContext, E extends Throwable> R accept(
      SimpleExtension.Function fnDef, int argIdx, FuncArgVisitor<R, C, E> fnArgVisitor, C context)
      throws E;

  interface FuncArgVisitor<R, C extends VisitationContext, E extends Throwable> {
    R visitExpr(SimpleExtension.Function fnDef, int argIdx, Expression e, C context) throws E;

    R visitType(SimpleExtension.Function fnDef, int argIdx, Type t, C context) throws E;

    R visitEnumArg(SimpleExtension.Function fnDef, int argIdx, EnumArg e, C context) throws E;
  }

  static FuncArgVisitor<FunctionArgument, EmptyVisitationContext, RuntimeException> toProto(
      final TypeExpressionVisitor<io.substrait.proto.Type, RuntimeException> typeVisitor,
      final ExpressionVisitor<
              io.substrait.proto.Expression, EmptyVisitationContext, RuntimeException>
          expressionVisitor) {

    return new FuncArgVisitor<FunctionArgument, EmptyVisitationContext, RuntimeException>() {

      @Override
      public FunctionArgument visitExpr(
          final SimpleExtension.Function fnDef,
          final int argIdx,
          final Expression e,
          final EmptyVisitationContext context)
          throws RuntimeException {
        final io.substrait.proto.Expression pE = e.accept(expressionVisitor, context);
        return FunctionArgument.newBuilder().setValue(pE).build();
      }

      @Override
      public FunctionArgument visitType(
          final SimpleExtension.Function fnDef,
          final int argIdx,
          final Type t,
          final EmptyVisitationContext context)
          throws RuntimeException {
        final io.substrait.proto.Type pTyp = t.accept(typeVisitor);
        return FunctionArgument.newBuilder().setType(pTyp).build();
      }

      @Override
      public FunctionArgument visitEnumArg(
          final SimpleExtension.Function fnDef,
          final int argIdx,
          final EnumArg ea,
          final EmptyVisitationContext context)
          throws RuntimeException {
        FunctionArgument.Builder enumBldr = FunctionArgument.newBuilder();

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
    private final ProtoTypeConverter protoTypeConverter;

    public ProtoFrom(
        final ProtoExpressionConverter protoExprConverter,
        final ProtoTypeConverter protoTypeConverter) {
      this.protoExprConverter = protoExprConverter;
      this.protoTypeConverter = protoTypeConverter;
    }

    public FunctionArg convert(
        final SimpleExtension.Function funcDef, final int argIdx, final FunctionArgument fArg) {
      switch (fArg.getArgTypeCase()) {
        case TYPE:
          return protoTypeConverter.from(fArg.getType());
        case VALUE:
          return protoExprConverter.from(fArg.getValue());
        case ENUM:
          {
            final SimpleExtension.EnumArgument enumArgDef =
                (SimpleExtension.EnumArgument) funcDef.args().get(argIdx);
            final String optionValue = fArg.getEnum();
            return EnumArg.of(enumArgDef, optionValue);
          }
        default:
          throw new UnsupportedOperationException(
              String.format("Unable to convert FunctionArgument %s.", fArg));
      }
    }
  }
}
