package io.substrait.isthmus;

import io.substrait.extension.ResolvedAggregateBinding;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSingletonAggFunction;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.SqlStaticAggFunction;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlMinMaxAggFunction;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.Optionality;

/**
 * Provides Substrait-specific variants of Calcite aggregate functions to ensure type inference
 * matches Substrait expectations.
 *
 * <p>Default Calcite implementations may infer return types that differ from Substrait, causing
 * conversion issues. This class overrides those behaviors.
 */
public class AggregateFunctions {

  /** Substrait-specific MIN aggregate function (nullable return type). */
  public static final SqlAggFunction MIN = new SubstraitSqlMinMaxAggFunction(SqlKind.MIN);

  /** Substrait-specific MAX aggregate function (nullable return type). */
  public static final SqlAggFunction MAX = new SubstraitSqlMinMaxAggFunction(SqlKind.MAX);

  /** Substrait-specific AVG aggregate function (nullable return type). */
  public static final SqlAggFunction AVG = new SubstraitAvgAggFunction(SqlKind.AVG);

  /**
   * Standard deviation (population) aggregate function. Maps to Substrait's std_dev function with
   * distribution=POPULATION enum argument.
   */
  public static SqlAggFunction STDDEV_POP = new SubstraitAvgAggFunction(SqlKind.STDDEV_POP);

  /**
   * Standard deviation (sample) aggregate function. Maps to Substrait's std_dev function with
   * distribution=SAMPLE enum argument.
   */
  public static SqlAggFunction STDDEV_SAMP = new SubstraitAvgAggFunction(SqlKind.STDDEV_SAMP);

  /**
   * Variance (population) aggregate function. Maps to Substrait's variance function with
   * distribution=POPULATION enum argument.
   */
  public static SqlAggFunction VAR_POP = new SubstraitAvgAggFunction(SqlKind.VAR_POP);

  /**
   * Variance (sample) aggregate function. Maps to Substrait's variance function with
   * distribution=SAMPLE enum argument.
   */
  public static SqlAggFunction VAR_SAMP = new SubstraitAvgAggFunction(SqlKind.VAR_SAMP);

  /** Substrait-specific SUM aggregate function (nullable return type). */
  public static final SqlAggFunction SUM = new SubstraitSumAggFunction();

  /** Substrait-specific SUM0 aggregate function (non-null BIGINT return type). */
  public static final SqlAggFunction SUM0 = new SubstraitSumEmptyIsZeroAggFunction();

  /**
   * Wraps a configured aggregate function so it carries a resolved Substrait binding and infers the
   * given output type.
   *
   * <p>This is a transport adapter: Calcite validates an {@code AggregateCall}'s stored type
   * against the function's inference rule and {@code RelBuilder} re-infers it when copying a call,
   * so the chosen output type has to travel with the function. The operator's identity is the
   * binding alone (see {@link #boundBinding}); the carried type is readable via {@link
   * #declaredOutputType}.
   *
   * @param aggFunction configured aggregate function to adapt
   * @param binding resolved Substrait aggregate binding
   * @param outputType Calcite output type the wrapper should infer (e.g. the plan-declared type)
   * @return an aggregate function that carries the binding and infers the given output type
   */
  public static SqlAggFunction bind(
      SqlAggFunction aggFunction, ResolvedAggregateBinding binding, RelDataType outputType) {
    return new BoundSqlAggFunction(unwrapBound(aggFunction), binding, outputType);
  }

  /**
   * Returns the configured aggregate function underlying a {@link #bind bound} function, or the
   * input unchanged.
   *
   * @param aggFunction aggregate function to inspect
   * @return the underlying configured aggregate function
   */
  public static SqlAggFunction unwrapBound(SqlAggFunction aggFunction) {
    return aggFunction instanceof BoundSqlAggFunction
        ? ((BoundSqlAggFunction) aggFunction).delegate
        : aggFunction;
  }

  /**
   * Returns the given aggregate with every {@link #bind bound} call replaced by a call on the
   * configured function it wraps, its type re-inferred by that function.
   *
   * <p>Use this before <em>executing</em> a converted plan. Calcite dispatches aggregate
   * implementations by operator identity ({@code RexImpTable} keyed on the {@link SqlAggFunction},
   * which compares class, name and kind), so a bound function has no implementor and Enumerable or
   * Bindable execution rejects the aggregate. The same already holds for the Substrait variants in
   * this class — {@link #SUM}, {@link #MIN}, {@link #AVG} and the rest are distinct classes from
   * the stock operators — so executability was never a property of a converted aggregate plan;
   * unwrapping restores it for the calls that can have it.
   *
   * <p>Unwrapping is an honest loss: the binding is dropped, and each call's type becomes the
   * underlying function's inference again — the type {@code
   * AggregateConversion.OutputTypeSource#CALCITE_INFERENCE} would have chosen up front. The carried
   * type cannot be kept on the unwrapped call, because {@code Aggregate}'s constructor asserts that
   * a call's stored type matches what its operator infers.
   *
   * @param aggregate aggregate whose calls to unwrap
   * @return an aggregate with no bound calls, or the same instance if it had none
   */
  public static Aggregate unwrapBound(Aggregate aggregate) {
    List<AggregateCall> unwrappedCalls = new ArrayList<>();
    boolean changed = false;
    for (AggregateCall call : aggregate.getAggCallList()) {
      SqlAggFunction unwrapped = unwrapBound(call.getAggregation());
      if (unwrapped == call.getAggregation()) {
        unwrappedCalls.add(call);
        continue;
      }
      changed = true;
      unwrappedCalls.add(
          AggregateCall.create(
              unwrapped,
              call.isDistinct(),
              call.isApproximate(),
              call.ignoreNulls(),
              call.rexList,
              call.getArgList(),
              call.filterArg,
              call.distinctKeys,
              call.getCollation(),
              aggregate.hasEmptyGroup(),
              aggregate.getInput(),
              null,
              call.getName()));
    }
    return changed
        ? aggregate.copy(
            aggregate.getTraitSet(),
            aggregate.getInput(),
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            unwrappedCalls)
        : aggregate;
  }

  /**
   * Returns the resolved Substrait aggregate binding carried by the given function, if any.
   *
   * @param aggFunction aggregate function to inspect
   * @return the resolved binding, or empty if the function does not carry one
   */
  public static Optional<ResolvedAggregateBinding> boundBinding(SqlAggFunction aggFunction) {
    return aggFunction instanceof BoundSqlAggFunction
        ? Optional.of(((BoundSqlAggFunction) aggFunction).binding)
        : Optional.empty();
  }

  /**
   * Returns the output type a {@link #bind bound} function carries, if any. This is the type the
   * wrapper infers — under {@code PLAN_OUTPUT} the type declared by the plan, which may differ from
   * the one derived from the extension declaration when the plan is not validated against it.
   *
   * @param aggFunction aggregate function to inspect
   * @return the carried output type, or empty if the function does not carry one
   */
  public static Optional<RelDataType> declaredOutputType(SqlAggFunction aggFunction) {
    return aggFunction instanceof BoundSqlAggFunction
        ? Optional.of(((BoundSqlAggFunction) aggFunction).outputType)
        : Optional.empty();
  }

  /**
   * Converts default Calcite aggregate functions to Substrait-specific variants when needed.
   *
   * @param aggFunction the Calcite aggregate function
   * @return optional containing Substrait equivalent if conversion applies
   */
  public static Optional<SqlAggFunction> toSubstraitAggVariant(SqlAggFunction aggFunction) {
    // First check by SqlKind to handle all statistical functions
    SqlKind kind = aggFunction.getKind();
    switch (kind) {
      case MIN:
        return Optional.of(AggregateFunctions.MIN);
      case MAX:
        return Optional.of(AggregateFunctions.MAX);
      case AVG:
        return Optional.of(AggregateFunctions.AVG);
      case STDDEV_POP:
        return Optional.of(AggregateFunctions.STDDEV_POP);
      case STDDEV_SAMP:
        return Optional.of(AggregateFunctions.STDDEV_SAMP);
      case VAR_POP:
        return Optional.of(AggregateFunctions.VAR_POP);
      case VAR_SAMP:
        return Optional.of(AggregateFunctions.VAR_SAMP);
      case SUM:
      case SUM0:
        // Check instance type for SUM variants
        if (aggFunction instanceof SqlSumEmptyIsZeroAggFunction) {
          return Optional.of(AggregateFunctions.SUM0);
        } else if (aggFunction instanceof SqlSumAggFunction) {
          return Optional.of(AggregateFunctions.SUM);
        }
        return Optional.empty();
      default:
        return Optional.empty();
    }
  }

  /** Substrait variant of {@link SqlMinMaxAggFunction} that forces nullable return type. */
  private static class SubstraitSqlMinMaxAggFunction extends SqlMinMaxAggFunction {
    public SubstraitSqlMinMaxAggFunction(SqlKind kind) {
      super(kind);
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      return ReturnTypes.ARG0_FORCE_NULLABLE.inferReturnType(opBinding);
    }
  }

  /** Substrait variant of {@link SqlSumAggFunction} that forces nullable return type. */
  private static class SubstraitSumAggFunction extends SqlSumAggFunction {
    public SubstraitSumAggFunction() {
      super(null); // Matches Calcite's instantiation pattern
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      return ReturnTypes.ARG0_FORCE_NULLABLE.inferReturnType(opBinding);
    }
  }

  /** Substrait variant of {@link SqlAvgAggFunction} that forces nullable return type. */
  private static class SubstraitAvgAggFunction extends SqlAvgAggFunction {
    public SubstraitAvgAggFunction(SqlKind kind) {
      super(kind);
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      return ReturnTypes.ARG0_FORCE_NULLABLE.inferReturnType(opBinding);
    }
  }

  /**
   * Substrait variant of {@link SqlSumEmptyIsZeroAggFunction} that forces BIGINT return type and
   * uses a user-friendly name.
   */
  private static class SubstraitSumEmptyIsZeroAggFunction
      extends org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction {
    public SubstraitSumEmptyIsZeroAggFunction() {
      super();
    }

    @Override
    public String getName() {
      // Override default `$sum0` with `sum0` for readability
      return "sum0";
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      return ReturnTypes.BIGINT.inferReturnType(opBinding);
    }
  }

  /**
   * Aggregate function that carries a resolved Substrait binding and infers a fixed output type.
   * All non-type behavior is delegated to the configured function.
   *
   * <p>The carried type has to travel on the operator's return-type inference; storing it on the
   * {@link org.apache.calcite.rel.core.AggregateCall} alone cannot work. {@code RelBuilder}
   * re-creates every pre-built call with no stored type, so the operator's inference decides the
   * type again — and more fundamentally, {@code Aggregate}'s constructor asserts that a call's
   * stored type matches what its operator infers ({@code typeMatchesInferred}), so a stock operator
   * carrying a divergent stored type is rejected outright, whether the aggregate is built through
   * {@code RelBuilder} or directly.
   *
   * <p>Identity is the {@link ResolvedAggregateBinding} alone, so that operator equality stays a
   * question of <em>which function</em> is being called and never of which type it produces: two
   * calls of the same Substrait function remain the same operator even when their plan-declared
   * types differ, while calls resolving to different functions (different extension, options or
   * enum arguments) stay distinct. Keeping two such calls apart in a single aggregate — {@code
   * AggregateCall} equality ignores the stored type, so {@code RelBuilder} would otherwise
   * deduplicate them — is the converter's job, not this operator's.
   *
   * <p>A bound function is a plan-interchange device, not an executable operator: Calcite looks up
   * aggregate implementations by operator identity, so a plan containing one cannot be run by the
   * Enumerable or Bindable convention. {@link #unwrapBound(Aggregate)} trades the binding back for
   * executability. (The Substrait variants above are already distinct classes from the stock
   * operators, so the same restriction applies to them.)
   *
   * <p>Type-rederiving transformations that expose a {@link SqlAggFunction} hook are disabled until
   * they become binding-aware: rollup ({@link #getRollup()}), splitting ({@link
   * SqlSplittableAggFunction}), singleton flattening ({@link SqlSingletonAggFunction}, which {@code
   * AggregateRemoveRule} unwraps) and static flattening ({@link SqlStaticAggFunction}, through
   * which that rule and {@code RelBuilder}'s already-unique optimization rewrite the call into a
   * constant) all recompute the transformed call's type from the underlying function or discard the
   * call altogether, which would lose the carried type or binding. Rules that instead match
   * directly on {@link org.apache.calcite.sql.SqlKind} (e.g. {@code AGGREGATE_REDUCE_FUNCTIONS})
   * have no such hook and are retained on the delegate's kind; a caller optimizing a converted plan
   * with a bound aggregate should either avoid such rules or make them binding-aware, otherwise
   * they may rewrite the call and drop the carried type.
   */
  private static final class BoundSqlAggFunction extends SqlAggFunction {
    private final SqlAggFunction delegate;
    private final ResolvedAggregateBinding binding;
    private final RelDataType outputType;

    private BoundSqlAggFunction(
        SqlAggFunction delegate, ResolvedAggregateBinding binding, RelDataType outputType) {
      super(
          delegate.getName(),
          delegate.getSqlIdentifier(),
          delegate.getKind(),
          ReturnTypes.explicit(outputType),
          delegate.getOperandTypeInference(),
          delegate.getOperandTypeChecker(),
          delegate.getFunctionType(),
          delegate.requiresOrder(),
          delegate.requiresOver(),
          delegate.requiresGroupOrder());
      this.delegate = delegate;
      this.binding = binding;
      this.outputType = outputType;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
      return delegate.getOperandCountRange();
    }

    @Override
    public SqlSyntax getSyntax() {
      return delegate.getSyntax();
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
      delegate.unparse(writer, call, leftPrec, rightPrec);
    }

    @Override
    public Optionality getDistinctOptionality() {
      return delegate.getDistinctOptionality();
    }

    @Override
    public boolean allowsFilter() {
      return delegate.allowsFilter();
    }

    @Override
    public boolean allowsNullTreatment() {
      return delegate.allowsNullTreatment();
    }

    @Override
    public boolean skipsNullInputs() {
      return delegate.skipsNullInputs();
    }

    @Override
    public SqlAggFunction getRollup() {
      // A rollup re-infers the top aggregate's return type from the underlying function, which
      // would discard the canonical Substrait output type. Opt out until rollup is binding-aware.
      return null;
    }

    @Override
    public boolean isPercentile() {
      return delegate.isPercentile();
    }

    @Override
    public boolean allowsFraming() {
      return delegate.allowsFraming();
    }

    @Override
    public boolean isDeterministic() {
      // Volatility is the delegate's trait: inheriting SqlOperator's defaults would let Calcite
      // treat a volatile aggregate as stable and cache or collapse its results.
      return delegate.isDeterministic();
    }

    @Override
    public boolean isDynamicFunction() {
      return delegate.isDynamicFunction();
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
      if (SqlSingletonAggFunction.class.isAssignableFrom(clazz)
          || SqlStaticAggFunction.class.isAssignableFrom(clazz)) {
        // Splitting (e.g. pushing an aggregate through a join) re-infers the split calls' types
        // from the underlying function, discarding the canonical type. Opt out until it is
        // binding-aware. This covers the SqlSingletonAggFunction supertype as well, because
        // AggregateRemoveRule unwraps that rather than SqlSplittableAggFunction, and
        // SqlStaticAggFunction, through which that rule and RelBuilder's alreadyUnique
        // optimization flatten the aggregate into a Project — keeping the call's type but
        // dropping the binding, and with it the phase, options and exact declaration.
        return null;
      }
      if (clazz == ResolvedAggregateBinding.class) {
        // Expose the resolved binding so consumers (and the reverse converter) can recover the
        // original Substrait function without re-matching on the Calcite operator.
        return clazz.cast(binding);
      }
      T unwrapped = super.unwrap(clazz);
      return unwrapped != null ? unwrapped : delegate.unwrap(clazz);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof BoundSqlAggFunction)) {
        return false;
      }
      BoundSqlAggFunction other = (BoundSqlAggFunction) obj;
      // Identity is the semantic binding only: the carried output type is deliberately excluded so
      // that matching on the operator stays type-agnostic.
      return binding.equals(other.binding);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass(), binding);
    }
  }
}
