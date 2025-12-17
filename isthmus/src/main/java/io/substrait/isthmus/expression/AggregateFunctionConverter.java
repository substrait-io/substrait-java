package io.substrait.isthmus.expression;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.AggregateFunctions;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.type.Type;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * Converts Calcite {@link AggregateCall} instances into Substrait aggregate {@link
 * AggregateFunctionInvocation}s using configured function variants and signatures.
 *
 * <p>Handles special cases (e.g., approximate distinct count) and collation/sort fields.
 */
public class AggregateFunctionConverter
    extends FunctionConverter<
        SimpleExtension.AggregateFunctionVariant,
        AggregateFunctionInvocation,
        AggregateFunctionConverter.WrappedAggregateCall> {

  /**
   * Returns the supported aggregate signatures used for matching functions.
   *
   * @return immutable list of aggregate signatures
   */
  @Override
  protected ImmutableList<FunctionMappings.Sig> getSigs() {
    return FunctionMappings.AGGREGATE_SIGS;
  }

  /**
   * Creates a converter with the given function variants and type factory.
   *
   * @param functions available aggregate function variants
   * @param typeFactory Calcite type factory
   */
  public AggregateFunctionConverter(
      List<SimpleExtension.AggregateFunctionVariant> functions, RelDataTypeFactory typeFactory) {
    super(functions, typeFactory);
  }

  /**
   * Creates a converter with additional signatures and a type converter.
   *
   * @param functions available aggregate function variants
   * @param additionalSignatures extra signatures to consider
   * @param typeFactory Calcite type factory
   * @param typeConverter Substrait type converter
   */
  public AggregateFunctionConverter(
      List<SimpleExtension.AggregateFunctionVariant> functions,
      List<FunctionMappings.Sig> additionalSignatures,
      RelDataTypeFactory typeFactory,
      TypeConverter typeConverter) {
    super(functions, additionalSignatures, typeFactory, typeConverter);
  }

  /**
   * Builds a Substrait aggregate invocation from the matched call and arguments.
   *
   * @param call wrapped aggregate call
   * @param function matched Substrait function variant
   * @param arguments converted arguments
   * @param outputType result type of the invocation
   * @return aggregate function invocation
   */
  @Override
  protected AggregateFunctionInvocation generateBinding(
      WrappedAggregateCall call,
      SimpleExtension.AggregateFunctionVariant function,
      List<? extends FunctionArg> arguments,
      Type outputType) {
    AggregateCall agg = call.getUnderlying();

    List<Expression.SortField> sorts =
        agg.getCollation() != null
            ? agg.getCollation().getFieldCollations().stream()
                .map(r -> SubstraitRelVisitor.toSortField(r, call.inputType))
                .collect(java.util.stream.Collectors.toList())
            : Collections.emptyList();
    Expression.AggregationInvocation invocation =
        agg.isDistinct()
            ? Expression.AggregationInvocation.DISTINCT
            : Expression.AggregationInvocation.ALL;
    return ExpressionCreator.aggregateFunction(
        function,
        outputType,
        Expression.AggregationPhase.INITIAL_TO_RESULT,
        sorts,
        invocation,
        arguments);
  }

  /**
   * Attempts to convert a Calcite aggregate call to a Substrait invocation.
   *
   * @param input input relational node
   * @param inputType Substrait input struct type
   * @param call Calcite aggregate call
   * @param topLevelConverter converter for RexNodes to Expressions
   * @return optional Substrait aggregate invocation
   */
  public Optional<AggregateFunctionInvocation> convert(
      RelNode input,
      Type.Struct inputType,
      AggregateCall call,
      Function<RexNode, Expression> topLevelConverter) {

    FunctionFinder m = getFunctionFinder(call);
    if (m == null) {
      return Optional.empty();
    }
    if (!m.allowedArgCount(call.getArgList().size())) {
      return Optional.empty();
    }

    WrappedAggregateCall wrapped = new WrappedAggregateCall(call, input, rexBuilder, inputType);
    return m.attemptMatch(wrapped, topLevelConverter);
  }

  /**
   * Resolves the appropriate function finder, applying Substrait-specific variants when needed.
   *
   * @param call Calcite aggregate call
   * @return function finder for the resolved aggregate function, or {@code null} if none
   */
  protected FunctionFinder getFunctionFinder(AggregateCall call) {
    // replace COUNT() + distinct == true and approximate == true with APPROX_COUNT_DISTINCT
    // before converting into substrait function
    SqlAggFunction aggFunction = call.getAggregation();
    if (aggFunction == SqlStdOperatorTable.COUNT && call.isDistinct() && call.isApproximate()) {
      aggFunction = SqlStdOperatorTable.APPROX_COUNT_DISTINCT;
    }

    SqlAggFunction lookupFunction =
        // Replace default Calcite aggregate calls with Substrait specific variants.
        // See toSubstraitAggVariant for more details.
        AggregateFunctions.toSubstraitAggVariant(aggFunction).orElse(aggFunction);
    return signatures.get(lookupFunction);
  }

  /** Lightweight wrapper around {@link AggregateCall} providing operands and type access. */
  static class WrappedAggregateCall implements FunctionConverter.GenericCall {
    private final AggregateCall call;
    private final RelNode input;
    private final RexBuilder rexBuilder;
    private final Type.Struct inputType;

    /**
     * Creates a new wrapped aggregate call.
     *
     * @param call underlying Calcite aggregate call
     * @param input input relational node
     * @param rexBuilder Rex builder for operand construction
     * @param inputType Substrait input struct type
     */
    private WrappedAggregateCall(
        AggregateCall call, RelNode input, RexBuilder rexBuilder, Type.Struct inputType) {
      this.call = call;
      this.input = input;
      this.rexBuilder = rexBuilder;
      this.inputType = inputType;
    }

    /**
     * Returns operands as input references over the argument list.
     *
     * @return stream of RexNode operands
     */
    @Override
    public Stream<RexNode> getOperands() {
      return call.getArgList().stream().map(r -> rexBuilder.makeInputRef(input, r));
    }

    /**
     * Exposes the underlying Calcite aggregate call.
     *
     * @return the aggregate call
     */
    public AggregateCall getUnderlying() {
      return call;
    }

    /**
     * Returns the type of the aggregate call result.
     *
     * @return Calcite result type
     */
    @Override
    public RelDataType getType() {
      return call.getType();
    }
  }
}
