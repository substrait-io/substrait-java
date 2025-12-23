package io.substrait.isthmus;

import io.substrait.extension.SimpleExtension;
import io.substrait.relation.Rel;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;

/**
 * SubstraitToSql assists with converting Substrait to SQL
 *
 * <p>Conversion behaviours can be customized using a {@link ConverterProvider}
 */
public class SubstraitToSql extends SqlConverterBase {

  public SubstraitToSql() {
    this(new ConverterProvider());
  }

  /** Deprecated, use {@link #SubstraitToSql(ConverterProvider)} instead */
  public SubstraitToSql(SimpleExtension.ExtensionCollection extensionCollection) {
    this(new ConverterProvider(extensionCollection));
  }

  public SubstraitToSql(ConverterProvider converterProvider) {
    super(converterProvider);
  }

  public RelNode substraitRelToCalciteRel(Rel relRoot, Prepare.CatalogReader catalog) {
    return SubstraitRelNodeConverter.convert(relRoot, catalog, converterProvider);
  }
}
