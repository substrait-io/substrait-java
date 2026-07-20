package io.substrait.isthmus;

import java.util.Objects;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlMinMaxAggFunction;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.Optionality;
import org.checkerframework.checker.nullness.qual.Nullable;

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
   * Returns an aggregate function that infers the output type declared by a Substrait invocation.
   *
   * <p>Calcite validates an {@code AggregateCall}'s stored type against the function's inference
   * rule and {@code RelBuilder} re-infers that type when it copies a call. Carrying the declared
   * type in the function therefore keeps the Substrait contract intact through both operations.
   *
   * @param aggFunction aggregate function resolved from the Substrait function declaration
   * @param outputType output type carried by the Substrait invocation
   * @return an equivalent aggregate function with the declared output type
   */
  public static SqlAggFunction withOutputType(SqlAggFunction aggFunction, RelDataType outputType) {
    return new DeclaredOutputSqlAggFunction(aggFunction, outputType);
  }

  /**
   * Returns the function wrapped by {@link #withOutputType}, or the input unchanged.
   *
   * <p>The Calcite-to-Substrait function matcher keys on the configured aggregate function
   * instances, so it must look through the per-invocation output-type wrapper.
   *
   * @param aggFunction aggregate function to inspect
   * @return the underlying configured aggregate function
   */
  public static SqlAggFunction withoutDeclaredOutputType(SqlAggFunction aggFunction) {
    SqlAggFunction current = aggFunction;
    while (current instanceof DeclaredOutputSqlAggFunction) {
      current = ((DeclaredOutputSqlAggFunction) current).delegate;
    }
    return current;
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
   * Per-invocation aggregate function whose inference returns the output type carried by Substrait.
   *
   * <p>All non-type behavior remains that of the configured function. Equality deliberately follows
   * the configured function only: output-type compatibility is a consumer policy and should not be
   * smuggled into Calcite operator identity.
   */
  private static final class DeclaredOutputSqlAggFunction extends SqlAggFunction {
    private final SqlAggFunction delegate;

    private DeclaredOutputSqlAggFunction(SqlAggFunction delegate, RelDataType outputType) {
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
    public @Nullable SqlAggFunction getRollup() {
      return delegate.getRollup();
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
    public <T> @Nullable T unwrap(Class<T> clazz) {
      T unwrapped = super.unwrap(clazz);
      return unwrapped != null ? unwrapped : delegate.unwrap(clazz);
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      return obj == this
          || obj instanceof DeclaredOutputSqlAggFunction
              && delegate.equals(((DeclaredOutputSqlAggFunction) obj).delegate);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass(), delegate);
    }
  }
}
