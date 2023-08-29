package io.substrait.isthmus.expression;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.ImmutableWindowBound;
import io.substrait.expression.WindowBound;
import io.substrait.extension.SimpleExtension;
import io.substrait.type.Type;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.type.SqlTypeName;

public class WindowFunctionConverter
    extends FunctionConverter<
        SimpleExtension.WindowFunctionVariant,
        Expression.WindowFunctionInvocation,
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
  protected Expression.WindowFunctionInvocation generateBinding(
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

    WindowBound lowerBound = toWindowBound(window.getLowerBound());
    WindowBound upperBound = toWindowBound(window.getUpperBound());

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

  public Optional<Expression.WindowFunctionInvocation> convert(
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

  private WindowBound toWindowBound(RexWindowBound rexWindowBound) {
    if (rexWindowBound.isCurrentRow()) {
      return WindowBound.CURRENT_ROW;
    }
    if (rexWindowBound.isUnbounded()) {
      var direction = findWindowBoundDirection(rexWindowBound);
      return ImmutableWindowBound.UnboundedWindowBound.builder().direction(direction).build();
    } else {
      var direction = findWindowBoundDirection(rexWindowBound);
      if (rexWindowBound.getOffset() instanceof RexLiteral literal
          && SqlTypeName.EXACT_TYPES.contains(literal.getTypeName())) {
        BigDecimal offset = (BigDecimal) literal.getValue4();
        return ImmutableWindowBound.BoundedWindowBound.builder()
            .direction(direction)
            .offset(offset.longValue())
            .build();
      }
      throw new IllegalArgumentException(
          String.format(
              "substrait only supports integer window offsets. Received: %s",
              rexWindowBound.getOffset().getKind()));
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
