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

/**
 * Converts Calcite window function calls ({@link RexOver}) into Substrait {@link
 * Expression.WindowFunctionInvocation}s using configured Substrait window function variants.
 *
 * <p>Handles partitioning, ordering, bounds (ROWS/RANGE, lower/upper), and DISTINCT/ALL invocation.
 */
public class WindowFunctionConverter
    extends FunctionConverter<
        SimpleExtension.WindowFunctionVariant,
        Expression.WindowFunctionInvocation,
        WindowFunctionConverter.WrappedWindowCall> {

  /**
   * Returns the supported window function signatures used for matching.
   *
   * @return immutable list of supported signatures.
   */
  @Override
  protected ImmutableList<FunctionMappings.Sig> getSigs() {
    return FunctionMappings.WINDOW_SIGS;
  }

  /**
   * Creates a converter with the provided window function variants.
   *
   * @param functions Supported Substrait window function variants.
   * @param typeFactory Calcite type factory for type handling.
   */
  public WindowFunctionConverter(
      List<SimpleExtension.WindowFunctionVariant> functions, RelDataTypeFactory typeFactory) {
    super(functions, typeFactory);
  }

  /**
   * Creates a converter with provided function variants and additional signatures.
   *
   * @param functions Supported Substrait window function variants.
   * @param additionalSignatures Extra signatures to consider during matching.
   * @param typeFactory Calcite type factory for type handling.
   * @param typeConverter Converter for Calcite/Substrait types.
   */
  public WindowFunctionConverter(
      List<SimpleExtension.WindowFunctionVariant> functions,
      List<FunctionMappings.Sig> additionalSignatures,
      RelDataTypeFactory typeFactory,
      TypeConverter typeConverter) {
    super(functions, additionalSignatures, typeFactory, typeConverter);
  }

  /**
   * Generates a bound Substrait window function invocation for a matched call.
   *
   * @param call Wrapped window call, including {@link RexOver} and expression converter.
   * @param function Selected Substrait function variant.
   * @param arguments Converted Substrait function arguments.
   * @param outputType Result type for the invocation.
   * @return Built {@link Expression.WindowFunctionInvocation}.
   */
  @Override
  protected Expression.WindowFunctionInvocation generateBinding(
      WrappedWindowCall call,
      SimpleExtension.WindowFunctionVariant function,
      List<? extends FunctionArg> arguments,
      Type outputType) {
    RexOver over = call.over;
    RexWindow window = over.getWindow();

    List<Expression> partitionExprs =
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

  /**
   * Attempts to convert a Calcite {@link RexOver} call into a Substrait window function invocation.
   *
   * <p>Resolves the corresponding Substrait aggregate function variant, checks arity using
   * signatures, and builds the invocation if match succeeds.
   *
   * @param over Calcite windowed aggregate call.
   * @param topLevelConverter Function converting top-level {@link RexNode}s to Substrait {@link
   *     Expression}s.
   * @param rexExpressionConverter Converter for nested {@link RexNode} expressions.
   * @return {@link Optional} containing the {@link Expression.WindowFunctionInvocation} if matched;
   *     otherwise empty.
   */
  public Optional<Expression.WindowFunctionInvocation> convert(
      RexOver over,
      Function<RexNode, Expression> topLevelConverter,
      RexExpressionConverter rexExpressionConverter) {
    SqlAggFunction aggFunction = over.getAggOperator();

    SqlAggFunction lookupFunction =
        AggregateFunctions.toSubstraitAggVariant(aggFunction).orElse(aggFunction);
    FunctionFinder m = signatures.get(lookupFunction);
    if (m == null) {
      return Optional.empty();
    }
    if (!m.allowedArgCount(over.getOperands().size())) {
      return Optional.empty();
    }

    WrappedWindowCall wrapped = new WrappedWindowCall(over, rexExpressionConverter);
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
