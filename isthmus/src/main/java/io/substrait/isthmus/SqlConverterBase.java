package io.substrait.isthmus;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
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
import org.apache.calcite.sql2rel.SqlToRelConverter;

public class SqlConverterBase {
  protected final ConverterProvider converterProvider;

  public static final CalciteConnectionConfig CONNECTION_CONFIG =
      CalciteConnectionConfig.DEFAULT.set(
          CalciteConnectionProperty.CASE_SENSITIVE, Boolean.FALSE.toString());

  final RelDataTypeFactory factory;
  final RelOptCluster relOptCluster;
  final CalciteConnectionConfig config;
  final SqlToRelConverter.Config converterConfig;

  final SqlParser.Config parserConfig;

  protected SqlConverterBase(ConverterProvider converterProvider) {
    this.converterProvider = converterProvider;
    this.factory = converterProvider.getTypeFactory();
    this.config = converterProvider.getCalciteConnectionConfig();
    this.converterConfig = converterProvider.getSqlToRelConverterConfig();
    VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of("hello"));
    this.relOptCluster = RelOptCluster.create(planner, new RexBuilder(factory));
    relOptCluster.setMetadataQuerySupplier(
        () -> {
          ProxyingMetadataHandlerProvider handler =
              new ProxyingMetadataHandlerProvider(DefaultRelMetadataProvider.INSTANCE);
          return new RelMetadataQuery(handler);
        });
    parserConfig = converterProvider.getSqlParserConfig();
  }
}
