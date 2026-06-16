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

  /**
   * Accepts a visitor for this function argument.
   *
   * @param <R> the return type
   * @param <C> the visitation context type
   * @param <E> the exception type that may be thrown
   * @param fnDef the function definition
   * @param argIdx the argument index
   * @param fnArgVisitor the visitor
   * @param context the visitation context
   * @return the result of the visit
   * @throws E if the visit fails
   */
  <R, C extends VisitationContext, E extends Throwable> R accept(
      SimpleExtension.Function fnDef, int argIdx, FuncArgVisitor<R, C, E> fnArgVisitor, C context)
      throws E;

  /**
   * Visitor over the concrete {@link FunctionArg} kinds (expression, type and enum argument).
   *
   * @param <R> the return type
   * @param <C> the visitation context type
   * @param <E> the exception type that may be thrown
   */
  interface FuncArgVisitor<R, C extends VisitationContext, E extends Throwable> {
    /**
     * Visits an {@link Expression} argument.
     *
     * @param fnDef the function definition
     * @param argIdx the argument index
     * @param e the expression argument
     * @param context the visitation context
     * @return the result of the visit
     * @throws E if the visit fails
     */
    R visitExpr(SimpleExtension.Function fnDef, int argIdx, Expression e, C context) throws E;

    /**
     * Visits a {@link Type} argument.
     *
     * @param fnDef the function definition
     * @param argIdx the argument index
     * @param t the type argument
     * @param context the visitation context
     * @return the result of the visit
     * @throws E if the visit fails
     */
    R visitType(SimpleExtension.Function fnDef, int argIdx, Type t, C context) throws E;

    /**
     * Visits an {@link EnumArg} argument.
     *
     * @param fnDef the function definition
     * @param argIdx the argument index
     * @param e the enum argument
     * @param context the visitation context
     * @return the result of the visit
     * @throws E if the visit fails
     */
    R visitEnumArg(SimpleExtension.Function fnDef, int argIdx, EnumArg e, C context) throws E;
  }

  /**
   * Creates a visitor that converts function arguments to their protobuf {@link FunctionArgument}
   * representation.
   *
   * @param typeVisitor visitor used to convert type arguments to proto
   * @param expressionVisitor visitor used to convert expression arguments to proto
   * @return a visitor producing proto function arguments
   */
  static FuncArgVisitor<FunctionArgument, EmptyVisitationContext, RuntimeException> toProto(
      TypeExpressionVisitor<io.substrait.proto.Type, RuntimeException> typeVisitor,
      ExpressionVisitor<io.substrait.proto.Expression, EmptyVisitationContext, RuntimeException>
          expressionVisitor) {

    return new FuncArgVisitor<FunctionArgument, EmptyVisitationContext, RuntimeException>() {

      @Override
      public FunctionArgument visitExpr(
          SimpleExtension.Function fnDef, int argIdx, Expression e, EmptyVisitationContext context)
          throws RuntimeException {
        io.substrait.proto.Expression pE = e.accept(expressionVisitor, context);
        return FunctionArgument.newBuilder().setValue(pE).build();
      }

      @Override
      public FunctionArgument visitType(
          SimpleExtension.Function fnDef, int argIdx, Type t, EmptyVisitationContext context)
          throws RuntimeException {
        io.substrait.proto.Type pTyp = t.accept(typeVisitor);
        return FunctionArgument.newBuilder().setType(pTyp).build();
      }

      @Override
      public FunctionArgument visitEnumArg(
          SimpleExtension.Function fnDef, int argIdx, EnumArg ea, EmptyVisitationContext context)
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

    /**
     * Creates a converter using the given expression and type converters.
     *
     * @param protoExprConverter converter for proto expression arguments
     * @param protoTypeConverter converter for proto type arguments
     */
    public ProtoFrom(
        ProtoExpressionConverter protoExprConverter, ProtoTypeConverter protoTypeConverter) {
      this.protoExprConverter = protoExprConverter;
      this.protoTypeConverter = protoTypeConverter;
    }

    /**
     * Converts a proto {@link FunctionArgument} into its POJO {@link FunctionArg}.
     *
     * @param funcDef the function definition the argument belongs to
     * @param argIdx the argument index
     * @param fArg the proto function argument to convert
     * @return the converted function argument
     */
    public FunctionArg convert(
        SimpleExtension.Function funcDef, int argIdx, FunctionArgument fArg) {
      switch (fArg.getArgTypeCase()) {
        case TYPE:
          return protoTypeConverter.from(fArg.getType());
        case VALUE:
          return protoExprConverter.from(fArg.getValue());
        case ENUM:
          {
            SimpleExtension.EnumArgument enumArgDef =
                (SimpleExtension.EnumArgument) funcDef.args().get(argIdx);
            String optionValue = fArg.getEnum();
            return EnumArg.of(enumArgDef, optionValue);
          }
        default:
          throw new UnsupportedOperationException(
              String.format("Unable to convert FunctionArgument %s.", fArg));
      }
    }
  }
}
