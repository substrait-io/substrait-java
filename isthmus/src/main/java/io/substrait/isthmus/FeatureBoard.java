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
   * @return the selected built-in Calcite SQL compatibility mode. For e.g. APPLY is only supported
   * in SQL Server mode. Calcite parser will throw an exception if the SQL statement contains
   * operators that are not supported by the selected mode.
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
