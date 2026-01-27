package io.substrait.isthmus;

import io.substrait.extension.SimpleExtension;
import io.substrait.relation.Rel;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;

/**
 * Converts Substrait {@link Rel} plans to Calcite {@link RelNode} and then to SQL.
 *
 * <p>Uses {@link SqlConverterBase} as the base for SQL conversion and supports optional extensions.
 */
public class SubstraitToSql extends SqlConverterBase {

  /** Creates a Substrait-to-SQL converter with default features. */
  public SubstraitToSql() {
    super(FEATURES_DEFAULT);
  }

  /**
   * Creates a Substrait-to-SQL converter with default features and custom extensions.
   *
   * @param extensions Substrait extension collection for function/operator mappings.
   */
  public SubstraitToSql(SimpleExtension.ExtensionCollection extensions) {
    super(FEATURES_DEFAULT, extensions);
  }

  /**
   * Converts a Substrait {@link Rel} to a Calcite {@link RelNode}.
   *
   * <p>This is the first step before generating SQL from Substrait plans.
   *
   * @param relRoot The Substrait relational root to convert.
   * @param catalog The Calcite catalog reader for schema resolution.
   * @return A Calcite {@link RelNode} representing the converted Substrait plan.
   */
  public RelNode substraitRelToCalciteRel(Rel relRoot, Prepare.CatalogReader catalog) {
    return SubstraitRelNodeConverter.convert(
        relRoot, relOptCluster, catalog, parserConfig, extensionCollection);
  }
}
