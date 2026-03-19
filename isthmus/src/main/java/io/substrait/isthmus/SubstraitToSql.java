package io.substrait.isthmus;

import io.substrait.extension.SimpleExtension;
import io.substrait.plan.Plan;
import io.substrait.plan.Plan.Root;
import io.substrait.relation.Rel;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;

/**
 * SubstraitToSql assists with converting Substrait to SQL
 *
 * <p>Conversion behaviours can be customized using a {@link ConverterProvider}
 */
public class SubstraitToSql extends SqlConverterBase {

  protected SubstraitToCalcite substraitToCalcite;

  public SubstraitToSql() {
    this(new ConverterProvider());
  }

  /** Deprecated, use {@link #SubstraitToSql(ConverterProvider)} instead */
  @Deprecated
  public SubstraitToSql(SimpleExtension.ExtensionCollection extensions) {
    this(new ConverterProvider(extensions));
  }

  public SubstraitToSql(ConverterProvider converterProvider) {
    super(converterProvider);
    substraitToCalcite = converterProvider.getSubstraitToCalcite();
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
    return SubstraitRelNodeConverter.convert(relRoot, catalog, converterProvider);
  }

  /**
   * Converts a Substrait {@link Plan} to a list of SQL strings in the given {@link SqlDialect}.
   *
   * @param plan the Substrait {@link Plan} to convert to SQL, must not be null
   * @param dialect the {@link SqlDialect} to generate the SQL strings for, must not be null
   * @return list containing a SQL string for each {@link Plan.Root} in {@code plan}
   */
  public List<String> convert(Plan plan, SqlDialect dialect) {
    List<String> result = new ArrayList<>();
    RelToSqlConverter relToSql = new RelToSqlConverter(dialect);

    for (Root root : plan.getRoots()) {
      result.add(
          relToSql
              .visitRoot(substraitToCalcite.convert(root).project(true))
              .asStatement()
              .toSqlString(dialect)
              .getSql());
    }

    return result;
  }
}
