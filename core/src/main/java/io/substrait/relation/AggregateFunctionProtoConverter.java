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

  public AggregateFunctionProtoConverter(ExtensionCollector functionCollector) {
    this.functionCollector = functionCollector;
    this.exprProtoConverter = new ExpressionProtoConverter(functionCollector, null);
    this.typeProtoConverter = new TypeProtoConverter(functionCollector);
  }

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
        .setFunctionReference(
            functionCollector.getFunctionReference(measure.getFunction().declaration()))
        .build();
  }
}
