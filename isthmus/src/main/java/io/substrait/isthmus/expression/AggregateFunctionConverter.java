package io.substrait.isthmus.expression;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.EnumArg;
import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.StatisticalDistribution;
import io.substrait.extension.ResolvedAggregateBinding;
import io.substrait.extension.ResolvedArgument;
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

    return AggregateFunctionInvocation.builder()
        .declaration(function)
        .outputType(outputType)
        .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
        .sort(sortFields(agg, call.inputType))
        .invocation(
            agg.isDistinct()
                ? Expression.AggregationInvocation.DISTINCT
                : Expression.AggregationInvocation.ALL)
        .addAllArguments(arguments)
        .build();
  }

  /**
   * Rebuilds the invocation from the binding a converted call carries, when that binding still
   * describes the call.
   *
   * <p>A binding names the exact declaration the plan used and the semantics Calcite cannot express
   * — the function's options and its aggregation phase — so it beats re-matching: matching can only
   * guess a declaration among equally-shaped candidates, always produces a full initial-to-result
   * aggregation with no options, and for a phase that consumes an intermediate state cannot match
   * at all, because the call's operands are then intermediate values rather than the declaration's
   * arguments.
   *
   * <p>The binding describes the plan as it was converted, and a planner rule may have rewritten
   * the call since. Staleness is therefore decided by comparing the arguments the binding recorded
   * with the ones the call now has — not by re-validating against the declaration, which cannot
   * judge a phase whose state is parameterized by the initial arguments it no longer sees (the
   * intermediate {@code STRUCT<DECIMAL<38,S>,i64>} of a decimal average, say). When the arguments
   * differ, this returns empty and the call is matched on its own terms, binding and all.
   */
  private Optional<AggregateFunctionInvocation> fromCarriedBinding(
      RelNode input,
      Type.Struct inputType,
      AggregateCall call,
      Function<RexNode, Expression> topLevelConverter) {
    Optional<ResolvedAggregateBinding> bound =
        AggregateFunctions.boundBinding(call.getAggregation());
    if (!bound.isPresent()
        || !(bound.get().function().declaration()
            instanceof SimpleExtension.AggregateFunctionVariant)) {
      return Optional.empty();
    }
    ResolvedAggregateBinding binding = bound.get();
    SimpleExtension.AggregateFunctionVariant declaration =
        (SimpleExtension.AggregateFunctionVariant) binding.function().declaration();

    List<Expression> operands =
        call.getArgList().stream()
            .map(index -> topLevelConverter.apply(rexBuilder.makeInputRef(input, index)))
            .collect(java.util.stream.Collectors.toList());
    Optional<List<FunctionArg>> arguments = alignArguments(binding, declaration, operands);
    if (!arguments.isPresent()) {
      return Optional.empty();
    }

    AggregateFunctionInvocation invocation =
        AggregateFunctionInvocation.builder()
            .declaration(declaration)
            .outputType(typeConverter.toSubstrait(call.getType()))
            .aggregationPhase(binding.phase())
            .sort(sortFields(call, inputType))
            // The invocation comes from the Calcite call rather than from the binding: a planner
            // rule may legitimately have dropped a redundant DISTINCT since the plan was converted.
            .invocation(
                call.isDistinct()
                    ? Expression.AggregationInvocation.DISTINCT
                    : Expression.AggregationInvocation.ALL)
            .addAllArguments(arguments.get())
            .options(binding.function().options())
            .build();

    List<ResolvedArgument> rebuilt =
        ResolvedAggregateBinding.resolve(invocation).function().arguments();
    return rebuilt.equals(binding.function().arguments())
        ? Optional.of(invocation)
        : Optional.empty();
  }

  /**
   * Puts the call's operands back into the declaration's argument order, restoring the enum
   * arguments — which are not Calcite operands — from the binding. Returns empty when the operands
   * no longer fit the declaration, which again means the binding does not describe this call.
   */
  private static Optional<List<FunctionArg>> alignArguments(
      ResolvedAggregateBinding binding,
      SimpleExtension.AggregateFunctionVariant declaration,
      List<Expression> operands) {
    List<ResolvedArgument> resolved = binding.function().arguments();
    List<FunctionArg> arguments = new ArrayList<>();
    int operandIndex = 0;
    for (int index = 0; index < declaration.args().size(); index++) {
      if (declaration.args().get(index) instanceof SimpleExtension.EnumArgument) {
        if (index >= resolved.size() || resolved.get(index).kind() != ResolvedArgument.Kind.ENUM) {
          return Optional.empty();
        }
        arguments.add(
            resolved.get(index).enumValue().map(EnumArg::of).orElse(EnumArg.UNSPECIFIED_ENUM_ARG));
      } else {
        if (operandIndex >= operands.size()) {
          return Optional.empty();
        }
        arguments.add(operands.get(operandIndex++));
      }
    }
    // A variadic declaration states its trailing argument once but accepts it repeatedly.
    while (operandIndex < operands.size()) {
      if (!declaration.variadic().isPresent()) {
        return Optional.empty();
      }
      arguments.add(operands.get(operandIndex++));
    }
    return Optional.of(arguments);
  }

  private static List<Expression.SortField> sortFields(AggregateCall call, Type.Struct inputType) {
    return call.getCollation() != null
        ? call.getCollation().getFieldCollations().stream()
            .map(collation -> SubstraitRelVisitor.toSortField(collation, inputType))
            .collect(java.util.stream.Collectors.toList())
        : Collections.emptyList();
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

    // A call converted from Substrait may carry its resolved binding; that is a better source than
    // re-matching the operator, and the only source for a phase Calcite cannot represent.
    Optional<AggregateFunctionInvocation> carried =
        fromCarriedBinding(input, inputType, call, topLevelConverter);
    if (carried.isPresent()) {
      return carried;
    }

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
    SqlAggFunction aggFunction = AggregateFunctions.unwrapBound(call.getAggregation());
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
