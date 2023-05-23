package io.substrait.isthmus.expression;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.ImmutableExpression;
import io.substrait.expression.ImmutableWindowBound;
import io.substrait.expression.WindowBound;
import io.substrait.expression.WindowFunctionInvocation;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.proto.AggregateFunction;
import io.substrait.relation.Aggregate;
import io.substrait.type.Type;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexWindowBound;

public class WindowFunctionConverter
    extends FunctionConverter<
        SimpleExtension.WindowFunctionVariant,
        WindowFunctionInvocation,
        WindowFunctionConverter.WrappedAggregateCall> {

  private AggregateFunctionConverter aggregateFunctionConverter;

  @Override
  protected ImmutableList<FunctionMappings.Sig> getSigs() {
    return FunctionMappings.WINDOW_SIGS;
  }

  public WindowFunctionConverter(
      List<SimpleExtension.WindowFunctionVariant> functions,
      RelDataTypeFactory typeFactory,
      AggregateFunctionConverter aggregateFunctionConverter) {
    super(functions, typeFactory);
    this.aggregateFunctionConverter = aggregateFunctionConverter;
  }

  @Override
  protected WindowFunctionInvocation generateBinding(
      WrappedAggregateCall call,
      SimpleExtension.WindowFunctionVariant function,
      List<FunctionArg> arguments,
      Type outputType) {
    AggregateCall agg = call.getUnderlying();

    List<Expression.SortField> sorts =
        agg.getCollation() != null
            ? agg.getCollation().getFieldCollations().stream()
                .map(r -> SubstraitRelVisitor.toSortField(r, call.inputType))
                .collect(java.util.stream.Collectors.toList())
            : Collections.emptyList();
    AggregateFunction.AggregationInvocation invocation =
        agg.isDistinct()
            ? AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_DISTINCT
            : AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_ALL;
    return ExpressionCreator.windowFunction(
        function,
        outputType,
        Expression.AggregationPhase.INITIAL_TO_RESULT,
        sorts,
        invocation,
        arguments);
  }

  public Expression.Window convert(
      RelNode input,
      Type.Struct inputType,
      RexOver over,
      Function<RexNode, Expression> topLevelConverter,
      RexExpressionConverter rexExpressionConverter) {

    var lowerBound = toWindowBound(over.getWindow().getLowerBound(), rexExpressionConverter);
    var upperBound = toWindowBound(over.getWindow().getUpperBound(), rexExpressionConverter);
    var sqlAggFunction = over.getAggOperator();
    var argList =
        over.getOperands().stream()
            .map(r -> ((RexSlot) r).getIndex())
            .collect(java.util.stream.Collectors.toList());
    boolean approximate = false;
    int filterArg = -1;
    var call =
        AggregateCall.create(
            sqlAggFunction,
            over.isDistinct(),
            approximate,
            over.ignoreNulls(),
            argList,
            filterArg,
            null,
            RelCollations.EMPTY,
            over.getType(),
            sqlAggFunction.getName());
    var windowBuilder = Expression.Window.builder();
    var aggregateFunctionInvocation =
        aggregateFunctionConverter.convert(input, inputType, call, topLevelConverter);
    boolean find = false;
    if (aggregateFunctionInvocation.isPresent()) {
      var aggMeasure =
          Aggregate.Measure.builder().function(aggregateFunctionInvocation.get()).build();
      windowBuilder.aggregateFunction(aggMeasure).hasNormalAggregateFunction(true);
      find = true;
    } else {
      // maybe it's a window function
      var windowFuncInvocation =
          findWindowFunctionInvocation(input, inputType, call, topLevelConverter);
      if (windowFuncInvocation.isPresent()) {
        var windowFunc =
            ImmutableExpression.WindowFunction.builder()
                .function(windowFuncInvocation.get())
                .build();
        windowBuilder.windowFunction(windowFunc).hasNormalAggregateFunction(false);
        find = true;
      }
    }
    if (!find) {
      throw new RuntimeException(
          String.format(
              "Not found the corresponding window aggregate function:%s", sqlAggFunction));
    }
    var window = over.getWindow();
    var partitionExps =
        window.partitionKeys.stream()
            .map(r -> r.accept(rexExpressionConverter))
            .collect(java.util.stream.Collectors.toList());
    var sortFields =
        window.orderKeys.stream()
            .map(r -> toSortField(r, rexExpressionConverter))
            .collect(java.util.stream.Collectors.toList());

    return windowBuilder
        .addAllOrderBy(sortFields)
        .addAllPartitionBy(partitionExps)
        .lowerBound(lowerBound)
        .upperBound(upperBound)
        .type(TypeConverter.convert(over.getType()))
        .build();
  }

  private Optional<WindowFunctionInvocation> findWindowFunctionInvocation(
      RelNode input,
      Type.Struct inputType,
      AggregateCall call,
      Function<RexNode, Expression> topLevelConverter) {
    FunctionFinder m = signatures.get(call.getAggregation());
    if (m == null) {
      return Optional.empty();
    }
    if (!m.allowedArgCount(call.getArgList().size())) {
      return Optional.empty();
    }

    var wrapped = new WrappedAggregateCall(call, input, rexBuilder, inputType);
    var windowFunctionInvocation = m.attemptMatch(wrapped, topLevelConverter);
    return windowFunctionInvocation;
  }

  private WindowBound toWindowBound(
      RexWindowBound rexWindowBound, RexExpressionConverter rexExpressionConverter) {
    if (rexWindowBound.isCurrentRow()) {
      return ImmutableWindowBound.CurrentRowWindowBound.builder().build();
    }
    if (rexWindowBound.isUnbounded()) {
      var direction = findWindowBoundDirection(rexWindowBound);
      return ImmutableWindowBound.UnboundedWindowBound.builder().direction(direction).build();
    } else {
      var direction = findWindowBoundDirection(rexWindowBound);
      var offset = rexWindowBound.getOffset().accept(rexExpressionConverter);
      return ImmutableWindowBound.BoundedWindowBound.builder()
          .direction(direction)
          .offset(offset)
          .build();
    }
  }

  private WindowBound.Direction findWindowBoundDirection(RexWindowBound rexWindowBound) {
    return rexWindowBound.isFollowing()
        ? WindowBound.Direction.FOLLOWING
        : WindowBound.Direction.PRECEDING;
  }

  private Expression.SortField toSortField(
      RexFieldCollation rexFieldCollation, RexExpressionConverter rexExpressionConverter) {
    var expr = rexFieldCollation.left.accept(rexExpressionConverter);
    var rexDirection = rexFieldCollation.getDirection();
    Expression.SortDirection direction =
        switch (rexDirection) {
          case ASCENDING -> rexFieldCollation.getNullDirection()
                  == RelFieldCollation.NullDirection.LAST
              ? Expression.SortDirection.ASC_NULLS_LAST
              : Expression.SortDirection.ASC_NULLS_FIRST;
          case DESCENDING -> rexFieldCollation.getNullDirection()
                  == RelFieldCollation.NullDirection.LAST
              ? Expression.SortDirection.DESC_NULLS_LAST
              : Expression.SortDirection.DESC_NULLS_FIRST;
          default -> throw new IllegalArgumentException(
              String.format(
                  "Unexpected RelFieldCollation.Direction:%s enum at the RexFieldCollation!",
                  rexDirection));
        };

    return Expression.SortField.builder().expr(expr).direction(direction).build();
  }

  static class WrappedAggregateCall implements FunctionConverter.GenericCall {
    private final AggregateCall call;
    private final RelNode input;
    private final RexBuilder rexBuilder;
    private final Type.Struct inputType;

    private WrappedAggregateCall(
        AggregateCall call, RelNode input, RexBuilder rexBuilder, Type.Struct inputType) {
      this.call = call;
      this.input = input;
      this.rexBuilder = rexBuilder;
      this.inputType = inputType;
    }

    @Override
    public Stream<RexNode> getOperands() {
      return call.getArgList().stream().map(r -> rexBuilder.makeInputRef(input, r));
    }

    public AggregateCall getUnderlying() {
      return call;
    }

    @Override
    public RelDataType getType() {
      return call.getType();
    }
  }
}
