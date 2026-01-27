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
   * @return Calcite's identifier casing policy for unquoted identifiers.
   */
  @Value.Default
  public Casing unquotedCasing() {
    return Casing.TO_UPPER;
  }

  /**
   * Controls whether to support dynamic user-defined functions (UDFs) during SQL to Substrait plan
   * conversion.
   *
   * <p>When enabled, custom functions defined in extension YAML files are available for use in SQL
   * queries. These functions will be dynamically converted to SQL operators during plan conversion.
   * This feature must be explicitly enabled by users and is disabled by default.
   *
   * @return true if dynamic UDFs should be supported; false otherwise (default)
   */
  @Value.Default
  public boolean allowDynamicUdfs() {
    return false;
  }

  /**
   * Controls whether to automatically create mappings for all unmapped functions using
   * SimpleExtensionToSqlOperator.
   *
   * <p>When enabled, functions from extension YAML files that are not explicitly mapped in
   * FunctionMappings will be automatically mapped to Calcite SqlOperators. This allows custom and
   * dynamic functions to be used in SQL queries without manual mapping configuration.
   *
   * <p>This feature is disabled by default for backward compatibility.
   *
   * @return true if automatic fallback to dynamic function mapping should be enabled; false
   *     otherwise (default)
   */
  @Value.Default
  public boolean autoFallbackToDynamicFunctionMapping() {
    return false;
  }
}
