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

/**
 * Base class for Substrait SQL conversion pipelines.
 *
 * <p>Configures Calcite parser, connection, planner, and cluster. Holds the Substrait extensions
 * and feature flags. Subclasses can build conversions from SQL to Calcite/Substrait using this
 * shared setup.
 */
public class SqlConverterBase {
  /** The converter provider containing configuration. */
  protected final ConverterProvider converterProvider;

  /** Default Calcite connection config (case-insensitive). */
  public static final CalciteConnectionConfig CONNECTION_CONFIG =
      CalciteConnectionConfig.DEFAULT.set(
          CalciteConnectionProperty.CASE_SENSITIVE, Boolean.FALSE.toString());

  /** Calcite type factory using the Substrait type system. */
  final RelDataTypeFactory factory;

  /** Calcite optimization cluster with planner, type factory, and RexBuilder. */
  final RelOptCluster relOptCluster;

  /** Connection configuration used for SQL parsing and validation. */
  final CalciteConnectionConfig config;

  /** Configuration for SQL-to-Rel conversion. */
  final SqlToRelConverter.Config converterConfig;

  /** Parser configuration, including casing and DDL parser factory. */
  final SqlParser.Config parserConfig;

  /**
   * Creates a converter base with explicit features and extensions.
   *
   * @param converterProvider Converter Provider for configuration
   */
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
