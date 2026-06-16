package io.substrait.relation;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.FunctionOption;
import io.substrait.expression.proto.ProtoExpressionConverter;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.ExtensionLookup;
import io.substrait.extension.SimpleExtension;
import io.substrait.type.proto.ProtoTypeConverter;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Converts from {@link io.substrait.proto.AggregateFunction} to {@link
 * io.substrait.relation.Aggregate.Measure}
 */
public class ProtoAggregateFunctionConverter {
  private final ExtensionLookup lookup;
  private final SimpleExtension.ExtensionCollection extensions;
  private final ProtoTypeConverter protoTypeConverter;
  private final ProtoExpressionConverter protoExpressionConverter;

  /**
   * Creates a converter using the default extension collection.
   *
   * @param lookup used to resolve function references to their declarations
   * @param protoExpressionConverter converter for nested proto expressions
   */
  public ProtoAggregateFunctionConverter(
      ExtensionLookup lookup, ProtoExpressionConverter protoExpressionConverter) {
    this(lookup, DefaultExtensionCatalog.DEFAULT_COLLECTION, protoExpressionConverter);
  }

  /**
   * Creates a converter using the given extension collection.
   *
   * @param lookup used to resolve function references to their declarations
   * @param extensions the extension collection providing function definitions
   * @param protoExpressionConverter converter for nested proto expressions
   */
  public ProtoAggregateFunctionConverter(
      ExtensionLookup lookup,
      SimpleExtension.ExtensionCollection extensions,
      ProtoExpressionConverter protoExpressionConverter) {
    this.lookup = lookup;
    this.extensions = extensions;
    this.protoTypeConverter = new ProtoTypeConverter(lookup, extensions);
    this.protoExpressionConverter = protoExpressionConverter;
  }

  /**
   * Converts a proto {@link io.substrait.proto.AggregateFunction} into its POJO {@link
   * AggregateFunctionInvocation}.
   *
   * @param measure the proto aggregate function to convert
   * @return the converted aggregate function invocation
   */
  public io.substrait.expression.AggregateFunctionInvocation from(
      io.substrait.proto.AggregateFunction measure) {
    FunctionArg.ProtoFrom protoFrom =
        new FunctionArg.ProtoFrom(protoExpressionConverter, protoTypeConverter);
    SimpleExtension.AggregateFunctionVariant aggregateFunction =
        lookup.getAggregateFunction(measure.getFunctionReference(), extensions);
    List<FunctionArg> functionArgs =
        IntStream.range(0, measure.getArgumentsCount())
            .mapToObj(i -> protoFrom.convert(aggregateFunction, i, measure.getArguments(i)))
            .collect(java.util.stream.Collectors.toList());
    List<FunctionOption> options =
        measure.getOptionsList().stream()
            .map(ProtoExpressionConverter::fromFunctionOption)
            .collect(Collectors.toList());
    List<Expression.SortField> sorts =
        measure.getSortsList().stream()
            .map(protoExpressionConverter::fromSortField)
            .collect(Collectors.toList());
    return AggregateFunctionInvocation.builder()
        .arguments(functionArgs)
        .declaration(aggregateFunction)
        .outputType(protoTypeConverter.from(measure.getOutputType()))
        .aggregationPhase(Expression.AggregationPhase.fromProto(measure.getPhase()))
        .invocation(Expression.AggregationInvocation.fromProto(measure.getInvocation()))
        .options(options)
        .sort(sorts)
        .build();
  }
}
