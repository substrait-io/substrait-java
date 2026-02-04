package io.substrait.isthmus;

import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlMinMaxAggFunction;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * Provides Substrait-specific variants of Calcite aggregate functions to ensure type inference
 * matches Substrait expectations.
 *
 * <p>Default Calcite implementations may infer return types that differ from Substrait, causing
 * conversion issues. This class overrides those behaviors.
 */
public class AggregateFunctions {

  /** Substrait-specific MIN aggregate function (nullable return type). */
  public static SqlAggFunction MIN = new SubstraitSqlMinMaxAggFunction(SqlKind.MIN);

  /** Substrait-specific MAX aggregate function (nullable return type). */
  public static SqlAggFunction MAX = new SubstraitSqlMinMaxAggFunction(SqlKind.MAX);

  /** Substrait-specific AVG aggregate function (nullable return type). */
  public static SqlAggFunction AVG = new SubstraitAvgAggFunction(SqlKind.AVG);

  /** Substrait-specific SUM aggregate function (nullable return type). */
  public static SqlAggFunction SUM = new SubstraitSumAggFunction();

  /** Substrait-specific SUM0 aggregate function (non-null BIGINT return type). */
  public static SqlAggFunction SUM0 = new SubstraitSumEmptyIsZeroAggFunction();

  /**
   * Converts default Calcite aggregate functions to Substrait-specific variants when needed.
   *
   * @param aggFunction the Calcite aggregate function
   * @return optional containing Substrait equivalent if conversion applies
   */
  public static Optional<SqlAggFunction> toSubstraitAggVariant(SqlAggFunction aggFunction) {
    if (aggFunction instanceof SqlMinMaxAggFunction) {
      SqlMinMaxAggFunction fun = (SqlMinMaxAggFunction) aggFunction;
      return Optional.of(
          fun.getKind() == SqlKind.MIN ? AggregateFunctions.MIN : AggregateFunctions.MAX);
    } else if (aggFunction instanceof SqlAvgAggFunction) {
      return Optional.of(AggregateFunctions.AVG);
    } else if (aggFunction instanceof SqlSumAggFunction) {
      return Optional.of(AggregateFunctions.SUM);
    } else if (aggFunction instanceof SqlSumEmptyIsZeroAggFunction) {
      return Optional.of(AggregateFunctions.SUM0);
    } else {
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
}
