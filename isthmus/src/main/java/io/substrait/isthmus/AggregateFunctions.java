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

public class AggregateFunctions {

  // For some arithmetic aggregate functions, the default Calcite aggregate function implementations
  // will infer return types that differ from those expected by Substrait.
  // This type mismatch can cause conversion and planning failures.

  public static SqlAggFunction MIN = new SubstraitSqlMinMaxAggFunction(SqlKind.MIN);
  public static SqlAggFunction MAX = new SubstraitSqlMinMaxAggFunction(SqlKind.MAX);
  public static SqlAggFunction AVG = new SubstraitAvgAggFunction(SqlKind.AVG);
  public static SqlAggFunction SUM = new SubstraitSumAggFunction();
  public static SqlAggFunction SUM0 = new SubstraitSumEmptyIsZeroAggFunction();

  /**
   * Some Calcite rules, like {@link
   * org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule}, introduce the default
   * Calcite aggregate functions into plans.
   *
   * <p>When converting these Calcite plans to Substrait, we need to convert the default Calcite
   * aggregate calls to the Substrait specific variants.
   *
   * <p>This function attempts to convert the given {@code aggFunction} to its Substrait equivalent
   *
   * @param aggFunction the {@link SqlAggFunction} to convert to a Substrait specific variant
   * @return an optional containing the Substrait equivalent of the given {@code aggFunction} if
   *     conversion was needed, empty otherwise.
   */
  public static Optional<SqlAggFunction> toSubstraitAggVariant(SqlAggFunction aggFunction) {
    if (aggFunction instanceof SqlMinMaxAggFunction fun) {
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

  /** Extension of {@link SqlMinMaxAggFunction} that ALWAYS infers a nullable return type */
  private static class SubstraitSqlMinMaxAggFunction extends SqlMinMaxAggFunction {
    public SubstraitSqlMinMaxAggFunction(SqlKind kind) {
      super(kind);
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      return ReturnTypes.ARG0_FORCE_NULLABLE.inferReturnType(opBinding);
    }
  }

  /** Extension of {@link SqlSumAggFunction} that ALWAYS infers a nullable return type */
  private static class SubstraitSumAggFunction extends SqlSumAggFunction {
    public SubstraitSumAggFunction() {
      // This is intentionally null
      // See the instantiation of SqlSumAggFunction in SqlStdOperatorTable
      super(null);
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      return ReturnTypes.ARG0_FORCE_NULLABLE.inferReturnType(opBinding);
    }
  }

  /** Extension of {@link SqlAvgAggFunction} that ALWAYS infers a nullable return type */
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
   * Extension of {@link SqlSumEmptyIsZeroAggFunction} that ALWAYS infers a NOT NULL BIGINT return
   * type
   */
  private static class SubstraitSumEmptyIsZeroAggFunction
      extends org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction {
    public SubstraitSumEmptyIsZeroAggFunction() {
      super();
    }

    @Override
    public String getName() {
      // the default name for this function is `$sum0`
      // override this to `sum0` which is a nicer name to use in queries
      return "sum0";
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      return ReturnTypes.BIGINT.inferReturnType(opBinding);
    }
  }
}
