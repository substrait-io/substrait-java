package io.substrait.isthmus;

import io.substrait.relation.Rel;
import java.util.List;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;

public class SubstraitToSql extends SqlConverterBase {

  public SubstraitToSql() {
    super(FEATURES_DEFAULT);
  }

  public RelNode substraitRelToCalciteRel(Rel relRoot, List<String> tables)
      throws SqlParseException {
    CalciteCatalogReader catalogReader = registerCreateTables(tables);
    return SubstraitRelNodeConverter.convert(relRoot, relOptCluster, catalogReader, parserConfig);
  }

  public RelNode substraitRelToCalciteRel(Rel relRoot, Prepare.CatalogReader catalog) {
    return SubstraitRelNodeConverter.convert(relRoot, relOptCluster, catalog, parserConfig);
  }
}
