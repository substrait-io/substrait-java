package io.substrait.isthmus;

import io.substrait.extension.SimpleExtension;
import io.substrait.relation.Rel;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;

public class SubstraitToSql extends SqlConverterBase {

  public SubstraitToSql() {
    super(FEATURES_DEFAULT);
  }

  public SubstraitToSql(SimpleExtension.ExtensionCollection extensions) {
    super(FEATURES_DEFAULT, extensions);
  }

  public RelNode substraitRelToCalciteRel(Rel relRoot, Prepare.CatalogReader catalog) {
    return SubstraitRelNodeConverter.convert(
        relRoot, relOptCluster, catalog, parserConfig, extensionCollection);
  }
}
