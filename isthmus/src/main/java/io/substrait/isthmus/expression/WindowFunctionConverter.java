package io.substrait.isthmus.expression;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.ImmutableWindowBound;
import io.substrait.expression.WindowBound;
import io.substrait.expression.WindowFunctionInvocation;
import io.substrait.extension.SimpleExtension;
import io.substrait.type.Type;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.rex.RexWindowBound;

public class WindowFunctionConverter
    extends FunctionConverter<
        SimpleExtension.WindowFunctionVariant,
        WindowFunctionInvocation,
        WindowFunctionConverter.WrappedWindowCall> {

  @Override
  protected ImmutableList<FunctionMappings.Sig> getSigs() {
    return FunctionMappings.WINDOW_SIGS;
  }

  public WindowFunctionConverter(
      List<SimpleExtension.WindowFunctionVariant> functions, RelDataTypeFactory typeFactory) {
    super(functions, typeFactory);
  }

  @Override
  protected WindowFunctionInvocation generateBinding(
      WrappedWindowCall call,
      SimpleExtension.WindowFunctionVariant function,
      List<FunctionArg> arguments,
      Type outputType) {
    RexOver over = call.over;
    RexWindow window = over.getWindow();

    var partitionExprs =
        window.partitionKeys.stream()
            .map(r -> r.accept(call.rexExpressionConverter))
            .collect(java.util.stream.Collectors.toList());

    List<Expression.SortField> sorts =
        window.orderKeys != null
            ? window.orderKeys.stream()
                .map(rfc -> toSortField(rfc, call.rexExpressionConverter))
                .collect(java.util.stream.Collectors.toList())
            : Collections.emptyList();
    Expression.AggregationInvocation invocation =
        over.isDistinct()
            ? Expression.AggregationInvocation.DISTINCT
            : Expression.AggregationInvocation.ALL;

    WindowBound lowerBound = toWindowBound(window.getLowerBound(), call.rexExpressionConverter);
    WindowBound upperBound = toWindowBound(window.getUpperBound(), call.rexExpressionConverter);

    return ExpressionCreator.windowFunction(
        function,
        outputType,
        Expression.AggregationPhase.INITIAL_TO_RESULT,
        sorts,
        invocation,
        partitionExprs,
        lowerBound,
        upperBound,
        arguments);
  }

  public Optional<WindowFunctionInvocation> convert(
      RexOver over,
      Function<RexNode, Expression> topLevelConverter,
      RexExpressionConverter rexExpressionConverter) {
    var aggFunction = over.getAggOperator();
    FunctionFinder m = signatures.get(aggFunction);
    if (m == null) {
      return Optional.empty();
    }
    if (!m.allowedArgCount(over.getOperands().size())) {
      return Optional.empty();
    }

    var wrapped = new WrappedWindowCall(over, rexExpressionConverter);
    return m.attemptMatch(wrapped, topLevelConverter);
  }

  private WindowBound toWindowBound(
      RexWindowBound rexWindowBound, RexExpressionConverter rexExpressionConverter) {
    if (rexWindowBound.isCurrentRow()) {
      return WindowBound.CURRENT_ROW;
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

  static class WrappedWindowCall implements FunctionConverter.GenericCall {
    private final RexOver over;
    private final RexExpressionConverter rexExpressionConverter;

    private WrappedWindowCall(RexOver over, RexExpressionConverter rexExpressionConverter) {
      this.over = over;
      this.rexExpressionConverter = rexExpressionConverter;
    }

    @Override
    public Stream<RexNode> getOperands() {
      return over.getOperands().stream();
    }

    @Override
    public RelDataType getType() {
      return over.getType();
    }
  }
}
