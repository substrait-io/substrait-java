package io.substrait.isthmus;

import io.substrait.relation.Rel;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;

public class SubstraitToSql extends SqlConverterBase {

  public SubstraitToSql() {
    super(FEATURES_DEFAULT);
  }

  public RelNode substraitRelToCalciteRel(final Rel relRoot, final Prepare.CatalogReader catalog) {
    return SubstraitRelNodeConverter.convert(relRoot, relOptCluster, catalog, parserConfig);
  }
}
