package io.substrait.isthmus;

import org.apache.calcite.avatica.util.Casing;
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
   * @return Calcite's identifier casing policy for unquoted identifiers.
   */
  @Value.Default
  public Casing unquotedCasing() {
    return Casing.TO_UPPER;
  }
}
