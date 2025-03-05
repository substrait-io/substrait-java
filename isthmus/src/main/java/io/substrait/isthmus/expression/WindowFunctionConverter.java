package io.substrait.isthmus.expression;

import static io.substrait.isthmus.expression.SortFieldConverter.toSortField;
import static io.substrait.isthmus.expression.WindowBoundConverter.toWindowBound;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.WindowBound;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.AggregateFunctions;
import io.substrait.isthmus.TypeConverter;
import io.substrait.type.Type;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.sql.SqlAggFunction;

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

  public WindowFunctionConverter(
      List<SimpleExtension.WindowFunctionVariant> functions,
      List<FunctionMappings.Sig> additionalSignatures,
      RelDataTypeFactory typeFactory,
      TypeConverter typeConverter) {
    super(functions, additionalSignatures, typeFactory, typeConverter);
  }

  @Override
  protected Expression.WindowFunctionInvocation generateBinding(
      WrappedWindowCall call,
      SimpleExtension.WindowFunctionVariant function,
      List<? extends FunctionArg> arguments,
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

    // Calcite only supports ROW or RANGE mode
    Expression.WindowBoundsType boundsType =
        window.isRows() ? Expression.WindowBoundsType.ROWS : Expression.WindowBoundsType.RANGE;
    WindowBound lowerBound = toWindowBound(window.getLowerBound());
    WindowBound upperBound = toWindowBound(window.getUpperBound());

    return ExpressionCreator.windowFunction(
        function,
        outputType,
        Expression.AggregationPhase.INITIAL_TO_RESULT,
        sorts,
        invocation,
        partitionExprs,
        boundsType,
        lowerBound,
        upperBound,
        arguments);
  }

  public Optional<Expression.WindowFunctionInvocation> convert(
      RexOver over,
      Function<RexNode, Expression> topLevelConverter,
      RexExpressionConverter rexExpressionConverter) {
    var aggFunction = over.getAggOperator();

    SqlAggFunction lookupFunction =
        AggregateFunctions.toSubstraitAggVariant(aggFunction).orElse(aggFunction);
    FunctionFinder m = signatures.get(lookupFunction);
    if (m == null) {
      return Optional.empty();
    }
    if (!m.allowedArgCount(over.getOperands().size())) {
      return Optional.empty();
    }

    var wrapped = new WrappedWindowCall(over, rexExpressionConverter);
    return m.attemptMatch(wrapped, topLevelConverter);
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
