package io.substrait.relation;

import io.substrait.expression.FunctionArg;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.AggregateFunction;
import io.substrait.proto.FunctionArgument;
import io.substrait.type.proto.TypeProtoConverter;
import io.substrait.util.EmptyVisitationContext;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Converts from {@link io.substrait.relation.Aggregate.Measure} to {@link
 * io.substrait.proto.AggregateFunction}
 */
public class AggregateFunctionProtoConverter {

  private final ExpressionProtoConverter exprProtoConverter;
  private final TypeProtoConverter typeProtoConverter;
  private final ExtensionCollector functionCollector;

  /**
   * Constructs a converter with the specified extension collector.
   *
   * @param functionCollector the extension collector for tracking function references
   */
  public AggregateFunctionProtoConverter(ExtensionCollector functionCollector) {
    this(
        functionCollector,
        new ExpressionProtoConverter(functionCollector, null),
        new TypeProtoConverter(functionCollector));
  }

  /**
   * Constructs a converter using the caller's expression and type converters.
   *
   * @param functionCollector the extension collector shared by the converters
   * @param exprProtoConverter the converter for arguments and sort expressions
   * @param typeProtoConverter the converter for argument and output types
   */
  public AggregateFunctionProtoConverter(
      ExtensionCollector functionCollector,
      ExpressionProtoConverter exprProtoConverter,
      TypeProtoConverter typeProtoConverter) {
    this.functionCollector = functionCollector;
    this.exprProtoConverter = exprProtoConverter;
    this.typeProtoConverter = typeProtoConverter;
  }

  /**
   * Converts an aggregate measure to its protobuf representation.
   *
   * @param measure the aggregate measure to convert
   * @return the protobuf aggregate function
   */
  public AggregateFunction toProto(Aggregate.Measure measure) {
    FunctionArg.FuncArgVisitor<FunctionArgument, EmptyVisitationContext, RuntimeException>
        argVisitor = FunctionArg.toProto(typeProtoConverter, exprProtoConverter);
    List<FunctionArg> args = measure.getFunction().arguments();
    SimpleExtension.AggregateFunctionVariant aggFuncDef = measure.getFunction().declaration();

    return AggregateFunction.newBuilder()
        .setPhase(measure.getFunction().aggregationPhase().toProto())
        .setInvocation(measure.getFunction().invocation().toProto())
        .setOutputType(measure.getFunction().getType().accept(typeProtoConverter))
        .addAllArguments(
            IntStream.range(0, args.size())
                .mapToObj(
                    i ->
                        args.get(i)
                            .accept(aggFuncDef, i, argVisitor, EmptyVisitationContext.INSTANCE))
                .collect(Collectors.toList()))
        .addAllSorts(
            measure.getFunction().sort().stream()
                .map(exprProtoConverter::toProto)
                .collect(Collectors.toList()))
        .setFunctionReference(
            functionCollector.getFunctionReference(measure.getFunction().declaration()))
        .addAllOptions(
            measure.getFunction().options().stream()
                .map(ExpressionProtoConverter::from)
                .collect(Collectors.toList()))
        .build();
  }
}
