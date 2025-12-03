package io.substrait.isthmus;

import java.util.Arrays;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.ProxyingMetadataHandlerProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.RelBuilder;

public class RelCreator {

  private final RelOptCluster cluster;
  private CatalogReader catalog;

  public RelCreator() {
    this(null);
  }

  public RelCreator(final CatalogReader catalogReader) {
    if (catalogReader == null) {
      final CalciteSchema schema = CalciteSchema.createRootSchema(false);
      catalog =
          new CalciteCatalogReader(
              schema,
              Arrays.asList(),
              SubstraitTypeSystem.TYPE_FACTORY,
              SqlConverterBase.CONNECTION_CONFIG);
    } else {
      catalog = catalogReader;
    }

    final VolcanoPlanner planner =
        new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.EMPTY_CONTEXT);
    cluster = RelOptCluster.create(planner, new RexBuilder(SubstraitTypeSystem.TYPE_FACTORY));
  }

  public RelRoot parse(final String sql) {

    try {
      final SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
      final SqlNode parsed = parser.parseQuery();
      cluster.setMetadataQuerySupplier(
          () ->
              new RelMetadataQuery(
                  new ProxyingMetadataHandlerProvider(DefaultRelMetadataProvider.INSTANCE)));
      final SqlValidator validator =
          new Validator(catalog, cluster.getTypeFactory(), SqlValidator.Config.DEFAULT);

      final SqlToRelConverter.Config converterConfig =
          SqlToRelConverter.config().withTrimUnusedFields(true).withExpand(false);
      final SqlToRelConverter converter =
          new SqlToRelConverter(
              null, validator, catalog, cluster, StandardConvertletTable.INSTANCE, converterConfig);
      final RelRoot root = converter.convertQuery(parsed, true, true);
      return root;
    } catch (final SqlParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public RelBuilder createRelBuilder() {
    return RelBuilder.proto(Contexts.EMPTY_CONTEXT).create(cluster, catalog);
  }

  public RexBuilder rex() {
    return cluster.getRexBuilder();
  }

  public RelDataTypeFactory typeFactory() {
    return cluster.getTypeFactory();
  }

  private static final class Validator extends SqlValidatorImpl {

    public Validator(
        final SqlValidatorCatalogReader catalogReader,
        final RelDataTypeFactory typeFactory,
        final Config config) {
      super(SqlStdOperatorTable.instance(), catalogReader, typeFactory, config);
    }
  }
}
