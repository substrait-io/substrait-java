package io.substrait.isthmus;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
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
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql2rel.SqlToRelConverter;

/**
 * Base class for Substrait SQL conversion pipelines.
 *
 * <p>Configures Calcite parser, connection, planner, and cluster. Holds the Substrait extensions
 * and feature flags. Subclasses can build conversions from SQL to Calcite/Substrait using this
 * shared setup.
 */
public class SqlConverterBase {
  /** Substrait extension collection used for function/operator mappings. */
  protected final SimpleExtension.ExtensionCollection extensionCollection;

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

  /** Default feature board if none is provided. */
  protected static final FeatureBoard FEATURES_DEFAULT = ImmutableFeatureBoard.builder().build();

  /** Feature flags controlling conversion behavior. */
  final FeatureBoard featureBoard;

  /**
   * Creates a converter base with explicit features and extensions.
   *
   * @param features Feature flags controlling behavior; if {@code null}, defaults are used.
   * @param extensionCollection Substrait extension collection for mapping functions/operators.
   */
  protected SqlConverterBase(
      FeatureBoard features, SimpleExtension.ExtensionCollection extensionCollection) {
    this.factory = SubstraitTypeSystem.TYPE_FACTORY;
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

    this.extensionCollection = extensionCollection;
  }

  /**
   * Creates a converter base with explicit features and the default Substrait extension catalog.
   *
   * @param features Feature flags controlling behavior; if {@code null}, defaults are used.
   */
  protected SqlConverterBase(FeatureBoard features) {
    this(features, DefaultExtensionCatalog.DEFAULT_COLLECTION);
  }
}
