package io.substrait.isthmus;

import io.substrait.extension.SimpleExtension;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.ProxyingMetadataHandlerProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql2rel.SqlToRelConverter;

class SqlConverterBase {
  protected static final SimpleExtension.ExtensionCollection EXTENSION_COLLECTION =
      SimpleExtension.loadDefaults();

  final RelDataTypeFactory factory;
  final RelOptCluster relOptCluster;
  final CalciteConnectionConfig config;
  final SqlToRelConverter.Config converterConfig;

  final SqlParser.Config parserConfig;

  protected static final FeatureBoard FEATURES_DEFAULT = ImmutableFeatureBoard.builder().build();
  final FeatureBoard featureBoard;

  protected SqlConverterBase(FeatureBoard features) {
    this.factory = new JavaTypeFactoryImpl(SubstraitTypeSystem.TYPE_SYSTEM);
    this.config =
        CalciteConnectionConfig.DEFAULT.set(CalciteConnectionProperty.CASE_SENSITIVE, "false");
    this.converterConfig = SqlToRelConverter.config().withTrimUnusedFields(true).withExpand(false);
    VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of("hello"));
    this.relOptCluster = RelOptCluster.create(planner, new RexBuilder(factory));
    relOptCluster.setMetadataQuerySupplier(
        () -> {
          ProxyingMetadataHandlerProvider handler =
              new ProxyingMetadataHandlerProvider(DefaultRelMetadataProvider.INSTANCE);
          return new RelMetadataQuery(handler);
        });
    featureBoard = features == null ? FEATURES_DEFAULT : features;
    parserConfig =
        SqlParser.Config.DEFAULT
            .withUnquotedCasing(featureBoard.unquotedCasing())
            .withParserFactory(SqlDdlParserImpl.FACTORY)
            .withConformance(SqlConformanceEnum.LENIENT);
  }
}
