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
      final List<SimpleExtension.WindowFunctionVariant> functions,
      final RelDataTypeFactory typeFactory) {
    super(functions, typeFactory);
  }

  public WindowFunctionConverter(
      final List<SimpleExtension.WindowFunctionVariant> functions,
      final List<FunctionMappings.Sig> additionalSignatures,
      final RelDataTypeFactory typeFactory,
      final TypeConverter typeConverter) {
    super(functions, additionalSignatures, typeFactory, typeConverter);
  }

  @Override
  protected Expression.WindowFunctionInvocation generateBinding(
      final WrappedWindowCall call,
      final SimpleExtension.WindowFunctionVariant function,
      final List<? extends FunctionArg> arguments,
      final Type outputType) {
    final RexOver over = call.over;
    final RexWindow window = over.getWindow();

    final List<Expression> partitionExprs =
        window.partitionKeys.stream()
            .map(r -> r.accept(call.rexExpressionConverter))
            .collect(java.util.stream.Collectors.toList());

    final List<Expression.SortField> sorts =
        window.orderKeys != null
            ? window.orderKeys.stream()
                .map(rfc -> toSortField(rfc, call.rexExpressionConverter))
                .collect(java.util.stream.Collectors.toList())
            : Collections.emptyList();
    final Expression.AggregationInvocation invocation =
        over.isDistinct()
            ? Expression.AggregationInvocation.DISTINCT
            : Expression.AggregationInvocation.ALL;

    // Calcite only supports ROW or RANGE mode
    final Expression.WindowBoundsType boundsType =
        window.isRows() ? Expression.WindowBoundsType.ROWS : Expression.WindowBoundsType.RANGE;
    final WindowBound lowerBound = toWindowBound(window.getLowerBound());
    final WindowBound upperBound = toWindowBound(window.getUpperBound());

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
      final RexOver over,
      final Function<RexNode, Expression> topLevelConverter,
      final RexExpressionConverter rexExpressionConverter) {
    final SqlAggFunction aggFunction = over.getAggOperator();

    final SqlAggFunction lookupFunction =
        AggregateFunctions.toSubstraitAggVariant(aggFunction).orElse(aggFunction);
    final FunctionFinder m = signatures.get(lookupFunction);
    if (m == null) {
      return Optional.empty();
    }
    if (!m.allowedArgCount(over.getOperands().size())) {
      return Optional.empty();
    }

    final WrappedWindowCall wrapped = new WrappedWindowCall(over, rexExpressionConverter);
    return m.attemptMatch(wrapped, topLevelConverter);
  }

  static class WrappedWindowCall implements FunctionConverter.GenericCall {
    private final RexOver over;
    private final RexExpressionConverter rexExpressionConverter;

    private WrappedWindowCall(
        final RexOver over, final RexExpressionConverter rexExpressionConverter) {
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
