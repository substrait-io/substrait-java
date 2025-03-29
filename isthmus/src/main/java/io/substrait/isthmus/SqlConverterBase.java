package io.substrait.isthmus;

import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.calcite.SubstraitTable;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import java.util.List;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.ProxyingMetadataHandlerProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql2rel.SqlToRelConverter;

class SqlConverterBase {
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

  protected static final SimpleExtension.ExtensionCollection EXTENSION_COLLECTION =
      SimpleExtension.loadDefaults();

  CalciteCatalogReader registerCreateTables(List<String> tables) throws SqlParseException {
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(rootSchema, List.of(), factory, config);
    if (tables != null) {
      for (String tableDef : tables) {
        List<SubstraitTable> tList =
            SubstraitCreateStatementParser.processCreateStatements(tableDef);
        for (SubstraitTable t : tList) {
          rootSchema.add(t.getName(), t);
        }
      }
    }
    return catalogReader;
  }

  CalciteCatalogReader registerSchema(String name, Schema schema) {
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
    if (schema != null) {
      rootSchema.add(name, schema);
      rootSchema = rootSchema.getSubSchema(name, false);
    }
    return new CalciteCatalogReader(rootSchema, List.of(), factory, config);
  }
}
