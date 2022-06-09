package io.substrait.expression;

import io.substrait.expression.proto.ProtoExpressionConverter;
import io.substrait.function.SimpleExtension;
import io.substrait.proto.FunctionArgument;
import io.substrait.type.Type;
import io.substrait.type.TypeVisitor;
import io.substrait.type.proto.FromProto;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;

public interface FunctionArg {

  <R, E extends Throwable> R acceptFuncArgVis(FuncArgVisitor<R, E> fnArgVisitor) throws E;

  interface FuncArgVisitor<R, E extends Throwable> {
    R visitExpr(Expression e) throws E;

    R visitType(Type t) throws E;

    R visitEnumArg(EnumArg e) throws E;
  }

  static <R, ER, TR, E extends Throwable> FuncArgVisitor<R, E> createFuncArgVisitor(
      TypeVisitor<TR, E> typeVisitor,
      Function<TR, R> convertTyp,
      ExpressionVisitor<ER, E> expressionVisitor,
      Function<ER, R> convertExpr,
      Function<EnumArg, R> convertEnum) {
    return new FuncArgVisitor<R, E>() {

      public R visitExpr(Expression e) throws E {
        return convertExpr.apply(e.accept(expressionVisitor));
      }

      @Override
      public R visitType(Type t) throws E {
        return convertTyp.apply(t.accept(typeVisitor));
      }

      @Override
      public R visitEnumArg(EnumArg e) throws E {
        return convertEnum.apply(e);
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
      return switch (fArg.getArgTypeCase()) {
        case TYPE -> FromProto.from(fArg.getType());
        case VALUE -> exprBuilder.from(fArg.getValue());
        case ENUM -> convert(funcDef, argIdx, fArg.getEnum());
        default -> throw new UnsupportedOperationException(
            String.format("Unable to convert FunctionArgument %s.", fArg));
      };
    }

    /**
     * Account for optional arguments before argument position. For example for `(opt, req, opt,
     * req)`: - for `(v1)` `v1` can be `arg0 or arg1` - for `(v1, v2)` `v2` can be `arg1, arg2,
     * arg3`
     *
     * <p>TODO: move this to `SimpleExtension.Function`, or at least an inner function in
     * `possiblePositions`
     *
     * @param args
     * @param argIdx
     * @return
     */
    private int possiblePositions(List<SimpleExtension.Argument> args, int argIdx) {
      int possiblePos =
          (int) IntStream.range(0, argIdx + 1).filter(i -> !args.get(i).required()).count();
      return Math.max(possiblePos, args.size() - argIdx);
    }

    private FunctionArg convert(
        SimpleExtension.Function funcDef, int argIdx, FunctionArgument.Enum pEnum) {
      List<SimpleExtension.Argument> args = funcDef.args();
      int possibleRange = possiblePositions(args, argIdx);

      Optional<SimpleExtension.EnumArgument> enumArgOp =
          args.subList(argIdx, argIdx + possibleRange).stream()
              .filter(a -> a instanceof SimpleExtension.EnumArgument)
              .map(SimpleExtension.EnumArgument.class::cast)
              .filter(e -> e.options().contains(pEnum.getSpecified()))
              .findFirst();

      return enumArgOp.map(ea -> EnumArg.of(ea, pEnum.getSpecified())).orElseThrow();
    }
  }
}
