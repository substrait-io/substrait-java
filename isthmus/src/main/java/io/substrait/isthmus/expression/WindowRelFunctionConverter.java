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
      final List<SimpleExtension.WindowFunctionVariant> functions,
      final RelDataTypeFactory typeFactory) {
    super(functions, typeFactory);
  }

  public WindowRelFunctionConverter(
      final List<SimpleExtension.WindowFunctionVariant> functions,
      final List<FunctionMappings.Sig> additionalSignatures,
      final RelDataTypeFactory typeFactory,
      final TypeConverter typeConverter) {
    super(functions, additionalSignatures, typeFactory, typeConverter);
  }

  @Override
  protected ConsistentPartitionWindow.WindowRelFunctionInvocation generateBinding(
      final WrappedWindowRelCall call,
      final SimpleExtension.WindowFunctionVariant function,
      final List<? extends FunctionArg> arguments,
      final Type outputType) {
    final Window.RexWinAggCall over = call.getWinAggCall();

    final Expression.AggregationInvocation invocation =
        over.distinct
            ? Expression.AggregationInvocation.DISTINCT
            : Expression.AggregationInvocation.ALL;

    // Calcite only supports ROW or RANGE mode
    final Expression.WindowBoundsType boundsType =
        call.isRows() ? Expression.WindowBoundsType.ROWS : Expression.WindowBoundsType.RANGE;
    final WindowBound lowerBound = toWindowBound(call.getLowerBound());
    final WindowBound upperBound = toWindowBound(call.getUpperBound());

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
      final Window.RexWinAggCall winAggCall,
      final RexWindowBound lowerBound,
      final RexWindowBound upperBound,
      final boolean isRows,
      final Function<RexNode, Expression> topLevelConverter) {
    final SqlAggFunction aggFunction = (SqlAggFunction) winAggCall.getOperator();

    final SqlAggFunction lookupFunction =
        AggregateFunctions.toSubstraitAggVariant(aggFunction).orElse(aggFunction);
    final FunctionFinder m = signatures.get(lookupFunction);
    if (m == null) {
      return Optional.empty();
    }
    if (!m.allowedArgCount(winAggCall.getOperands().size())) {
      return Optional.empty();
    }

    final WrappedWindowRelCall wrapped =
        new WrappedWindowRelCall(winAggCall, lowerBound, upperBound, isRows);
    return m.attemptMatch(wrapped, topLevelConverter);
  }

  static class WrappedWindowRelCall implements FunctionConverter.GenericCall {
    private final Window.RexWinAggCall winAggCall;
    private final RexWindowBound lowerBound;
    private final RexWindowBound upperBound;
    private final boolean isRows;

    private WrappedWindowRelCall(
        final Window.RexWinAggCall winAggCall,
        final RexWindowBound lowerBound,
        final RexWindowBound upperBound,
        final boolean isRows) {
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
