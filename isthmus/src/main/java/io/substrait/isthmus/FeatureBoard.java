package io.substrait.isthmus;

import io.substrait.isthmus.SubstraitRelVisitor.CrossJoinPolicy;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.immutables.value.Value;

/**
 * A feature board is a collection of flags that are enabled or configurations that control the
 * handling of a request to convert query [batch] to Substrait plans.
 */
@Value.Immutable
public abstract class FeatureBoard {

  /**
   * @return true if parsing sql batch (multiple input sql statements) is enabled
   */
  @Value.Default
  public boolean allowsSqlBatch() {
    return false;
  }

  /**
   * Returns Calcite's SQL conformance mode used for the current request. For e.g. APPLY is only
   * supported in SQL Server mode. Calcite's parser will throw an exception if the selected mode is
   * not SQL Server and the SQL statement contains APPLY.
   *
   * @return the selected built-in Calcite SQL compatibility mode.
   */
  @Value.Default
  public SqlConformanceEnum sqlConformanceMode() {
    return SqlConformanceEnum.DEFAULT;
  }

  /**
   * @return the selected cross join policy
   */
  @Value.Default
  public CrossJoinPolicy crossJoinPolicy() {
    return CrossJoinPolicy.KEEP_AS_CROSS_JOIN;
  }
}
