package io.substrait.isthmus.expression;

import static io.substrait.isthmus.expression.WindowBoundConverter.toWindowBound;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.WindowBound;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.AggregateFunctions;
import io.substrait.isthmus.TypeConverter;
import io.substrait.relation.ConsistentPartitionWindow;
import io.substrait.type.Type;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlAggFunction;

public class WindowRelFunctionConverter
    extends FunctionConverter<
        SimpleExtension.WindowFunctionVariant,
        ConsistentPartitionWindow.WindowRelFunctionInvocation,
        WindowRelFunctionConverter.WrappedWindowRelCall> {

  @Override
  protected ImmutableList<FunctionMappings.Sig> getSigs() {
    return FunctionMappings.WINDOW_SIGS;
  }

  public WindowRelFunctionConverter(
      List<SimpleExtension.WindowFunctionVariant> functions, RelDataTypeFactory typeFactory) {
    super(functions, typeFactory);
  }

  public WindowRelFunctionConverter(
      List<SimpleExtension.WindowFunctionVariant> functions,
      List<FunctionMappings.Sig> additionalSignatures,
      RelDataTypeFactory typeFactory,
      TypeConverter typeConverter) {
    super(functions, additionalSignatures, typeFactory, typeConverter);
  }

  @Override
  protected ConsistentPartitionWindow.WindowRelFunctionInvocation generateBinding(
      WrappedWindowRelCall call,
      SimpleExtension.WindowFunctionVariant function,
      List<? extends FunctionArg> arguments,
      Type outputType) {
    Window.RexWinAggCall over = call.getWinAggCall();

    Expression.AggregationInvocation invocation =
        over.distinct
            ? Expression.AggregationInvocation.DISTINCT
            : Expression.AggregationInvocation.ALL;

    // Calcite only supports ROW or RANGE mode
    Expression.WindowBoundsType boundsType =
        call.isRows() ? Expression.WindowBoundsType.ROWS : Expression.WindowBoundsType.RANGE;
    WindowBound lowerBound = toWindowBound(call.getLowerBound());
    WindowBound upperBound = toWindowBound(call.getUpperBound());

    return ExpressionCreator.windowRelFunction(
        function,
        outputType,
        Expression.AggregationPhase.INITIAL_TO_RESULT,
        invocation,
        boundsType,
        lowerBound,
        upperBound,
        arguments);
  }

  public Optional<ConsistentPartitionWindow.WindowRelFunctionInvocation> convert(
      Window.RexWinAggCall winAggCall,
      RexWindowBound lowerBound,
      RexWindowBound upperBound,
      boolean isRows,
      Function<RexNode, Expression> topLevelConverter) {
    var aggFunction = (SqlAggFunction) winAggCall.getOperator();

    SqlAggFunction lookupFunction =
        AggregateFunctions.toSubstraitAggVariant(aggFunction).orElse(aggFunction);
    FunctionFinder m = signatures.get(lookupFunction);
    if (m == null) {
      return Optional.empty();
    }
    if (!m.allowedArgCount(winAggCall.getOperands().size())) {
      return Optional.empty();
    }

    var wrapped = new WrappedWindowRelCall(winAggCall, lowerBound, upperBound, isRows);
    return m.attemptMatch(wrapped, topLevelConverter);
  }

  static class WrappedWindowRelCall implements GenericCall {
    private final Window.RexWinAggCall winAggCall;
    private final RexWindowBound lowerBound;
    private final RexWindowBound upperBound;
    private final boolean isRows;

    private WrappedWindowRelCall(
        Window.RexWinAggCall winAggCall,
        RexWindowBound lowerBound,
        RexWindowBound upperBound,
        boolean isRows) {
      this.winAggCall = winAggCall;
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
      this.isRows = isRows;
    }

    @Override
    public Stream<RexNode> getOperands() {
      return winAggCall.getOperands().stream();
    }

    @Override
    public RelDataType getType() {
      return winAggCall.getType();
    }

    public Window.RexWinAggCall getWinAggCall() {
      return winAggCall;
    }

    public RexWindowBound getLowerBound() {
      return lowerBound;
    }

    public RexWindowBound getUpperBound() {
      return upperBound;
    }

    public boolean isRows() {
      return isRows;
    }
  }
}
