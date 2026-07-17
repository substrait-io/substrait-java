package io.substrait.isthmus.expression;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.StatisticalDistribution;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.AggregateFunctions;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.type.Type;
import java.util.ArrayList;
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
import org.apache.calcite.sql.SqlKind;
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
   * <p>This method constructs an {@link AggregateFunctionInvocation} with appropriate configuration
   * including sort fields and invocation type (DISTINCT or ALL).
   *
   * <p><b>Statistical Functions:</b> For standard deviation and variance functions (STDDEV_POP,
   * STDDEV_SAMP, VAR_POP, VAR_SAMP), the population/sample distinction is carried by a leading
   * {@code distribution} {@link io.substrait.expression.EnumArg} argument. That argument is
   * synthesized as an operand in {@link #convert} so that the generic matcher resolves the enum-arg
   * function variant ({@code std_dev:req_*} / {@code variance:req_*}) and constructs the {@link
   * io.substrait.expression.EnumArg}; no special handling is required here.
   *
   * @param call wrapped aggregate call containing the Calcite aggregate information
   * @param function matched Substrait function variant from the extension catalog
   * @param arguments converted function arguments
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

    // For statistical aggregates (std_dev/variance) the SAMPLE/POPULATION distinction is carried
    // by a leading "distribution" enum argument. Synthesize it as an operand so the generic matcher
    // resolves the enum-arg function variant and builds the EnumArg.
    List<RexNode> leadingArgs = leadingEnumArgs(call);
    if (!m.allowedArgCount(call.getArgList().size() + leadingArgs.size())) {
      return Optional.empty();
    }

    WrappedAggregateCall wrapped =
        new WrappedAggregateCall(call, leadingArgs, input, rexBuilder, inputType);
    return m.attemptMatch(wrapped, topLevelConverter);
  }

  /**
   * Computes the synthetic leading operands to prepend to a Calcite aggregate call before matching.
   *
   * <p>For standard deviation and variance functions, Substrait carries the population/sample
   * distinction as a leading {@code distribution} enum argument, whereas Calcite encodes it in the
   * operator's {@link SqlKind}. This returns the matching {@link StatisticalDistribution} flag so
   * the generic matcher selects the {@code std_dev:req_*} / {@code variance:req_*} variant and
   * constructs the corresponding {@link io.substrait.expression.EnumArg}.
   *
   * @param call the Calcite aggregate call
   * @return the leading enum operands (a single distribution flag for statistical functions, empty
   *     otherwise)
   */
  private List<RexNode> leadingEnumArgs(AggregateCall call) {
    List<RexNode> leadingArgs = new ArrayList<>();
    switch (call.getAggregation().getKind()) {
      case STDDEV_SAMP:
      case VAR_SAMP:
        leadingArgs.add(rexBuilder.makeFlag(StatisticalDistribution.SAMPLE));
        break;
      case STDDEV_POP:
      case VAR_POP:
        leadingArgs.add(rexBuilder.makeFlag(StatisticalDistribution.POPULATION));
        break;
      default:
        break;
    }
    return leadingArgs;
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
    SqlAggFunction aggFunction =
        AggregateFunctions.withoutDeclaredOutputType(call.getAggregation());
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
    private final List<RexNode> leadingArgs;
    private final RelNode input;
    private final RexBuilder rexBuilder;
    private final Type.Struct inputType;

    /**
     * Creates a new wrapped aggregate call.
     *
     * @param call underlying Calcite aggregate call
     * @param leadingArgs synthetic operands (e.g. a {@code distribution} enum flag) prepended ahead
     *     of the field arguments during matching
     * @param input input relational node
     * @param rexBuilder Rex builder for operand construction
     * @param inputType Substrait input struct type
     */
    private WrappedAggregateCall(
        AggregateCall call,
        List<RexNode> leadingArgs,
        RelNode input,
        RexBuilder rexBuilder,
        Type.Struct inputType) {
      this.call = call;
      this.leadingArgs = leadingArgs;
      this.input = input;
      this.rexBuilder = rexBuilder;
      this.inputType = inputType;
    }

    /**
     * Returns operands as the synthetic leading operands followed by input references over the
     * argument list.
     *
     * @return stream of RexNode operands
     */
    @Override
    public Stream<RexNode> getOperands() {
      return Stream.concat(
          leadingArgs.stream(),
          call.getArgList().stream().map(r -> rexBuilder.makeInputRef(input, r)));
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
