package io.substrait.isthmus;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.calcite.SubstraitOperatorTable;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.CallConverters;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.FieldSelectionConverter;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.SqlArrayValueConstructorCallConverter;
import io.substrait.isthmus.expression.SqlMapValueConstructorCallConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.plan.ImmutableExecutionBehavior;
import io.substrait.plan.Plan;
import io.substrait.relation.Rel;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

/**
 * ConverterProvider provides a single-point of configuration for a number of conversions: {@code
 * SQl <-> Calcite <-> Substrait}
 *
 * <p>It is consumed by all conversion classes as their primary source of configuration.
 *
 * <p>{@link #DEFAULT} is a shared instance configured with reasonable system defaults, equivalent
 * to {@code builder().build()}.
 *
 * <p>For customized conversion behaviour — including supplying a full Calcite {@link
 * SqlParser.Config} for SQL parsing — use the {@link #builder()}.
 *
 * <p>More in-depth customization can be achieved by extending this class, as is done in {@link
 * DynamicConverterProvider}.
 */
public class ConverterProvider {

  /**
   * The default Calcite {@link SqlParser.Config} used by isthmus: {@link SqlParser.Config#DEFAULT}
   * with {@link Casing#TO_UPPER} unquoted-identifier casing, the {@link SqlDdlParserImpl} parser
   * factory (so {@code CREATE TABLE} statements parse), and {@link SqlConformanceEnum#LENIENT}
   * conformance.
   *
   * <p>This is the recommended starting point for a customized parser configuration: derive from it
   * with Calcite's {@code withXxx} methods and pass the result to {@link
   * Builder#sqlParserConfig(SqlParser.Config)}, e.g. {@code
   * DEFAULT_SQL_PARSER_CONFIG.withUnquotedCasing(Casing.UNCHANGED)}.
   */
  public static final SqlParser.Config DEFAULT_SQL_PARSER_CONFIG =
      SqlParser.Config.DEFAULT
          .withUnquotedCasing(Casing.TO_UPPER)
          .withParserFactory(SqlDdlParserImpl.FACTORY)
          .withConformance(SqlConformanceEnum.LENIENT);

  /**
   * A shared default {@link ConverterProvider} instance using all system defaults. Equivalent to
   * {@code builder().build()} but avoids redundant construction at every call site.
   *
   * <p>This instance is safe to share because {@link ConverterProvider} is effectively immutable
   * after construction — all fields are set only in constructors.
   */
  public static final ConverterProvider DEFAULT = builder().build();

  /** The Calcite type factory used for creating and managing data types. */
  protected RelDataTypeFactory typeFactory;

  /** The collection of Substrait extensions (functions and types) available for conversion. */
  protected final SimpleExtension.ExtensionCollection extensions;

  /** The Calcite SQL parser configuration, controlling parsing behaviour like identifier casing. */
  protected final SqlParser.Config sqlParserConfig;

  /** Converter for Substrait scalar functions. */
  protected ScalarFunctionConverter scalarFunctionConverter;

  /** Converter for Substrait aggregate functions. */
  protected AggregateFunctionConverter aggregateFunctionConverter;

  /** Converter for Substrait window functions. */
  protected WindowFunctionConverter windowFunctionConverter;

  /** Converter for Substrait types to Calcite types and vice versa. */
  protected TypeConverter typeConverter;

  /** The execution behavior configuration for plans created by this converter. */
  protected final Plan.ExecutionBehavior executionBehavior;

  /**
   * Creates a ConverterProvider with default extension collection and type factory. Uses {@link
   * DefaultExtensionCatalog#DEFAULT_COLLECTION} and {@link SubstraitTypeSystem#TYPE_FACTORY}.
   *
   * @deprecated Use {@link #builder()} (or the shared {@link #DEFAULT} instance) instead.
   */
  @Deprecated
  public ConverterProvider() {
    this(builder());
  }

  /**
   * Creates a ConverterProvider with the specified extension collection and default type factory.
   *
   * @param extensions the Substrait extension collection to use
   * @deprecated Use {@link #builder()} instead, e.g. {@code
   *     builder().extensions(extensions).build()}.
   */
  @Deprecated
  public ConverterProvider(SimpleExtension.ExtensionCollection extensions) {
    this(builder().extensions(extensions));
  }

  /**
   * Creates a ConverterProvider with the specified extension collection and type factory.
   *
   * @param extensions the Substrait extension collection to use
   * @param typeFactory the Calcite type factory to use
   * @deprecated Use {@link #builder()} instead, e.g. {@code
   *     builder().extensions(extensions).typeFactory(typeFactory).build()}.
   */
  @Deprecated
  public ConverterProvider(
      SimpleExtension.ExtensionCollection extensions, RelDataTypeFactory typeFactory) {
    this(builder().extensions(extensions).typeFactory(typeFactory));
  }

  /**
   * Creates a ConverterProvider with full customization of all components.
   *
   * @param typeFactory the Calcite type factory to use
   * @param extensions the Substrait extension collection to use
   * @param sfc the scalar function converter to use
   * @param afc the aggregate function converter to use
   * @param wfc the window function converter to use
   * @param tc the type converter to use
   * @deprecated Use {@link #builder()} instead; the growing set of components is more readably
   *     configured through the builder than through this positional constructor.
   */
  @Deprecated
  public ConverterProvider(
      RelDataTypeFactory typeFactory,
      SimpleExtension.ExtensionCollection extensions,
      ScalarFunctionConverter sfc,
      AggregateFunctionConverter afc,
      WindowFunctionConverter wfc,
      TypeConverter tc) {
    this(
        builder()
            .typeFactory(typeFactory)
            .extensions(extensions)
            .scalarFunctionConverter(sfc)
            .aggregateFunctionConverter(afc)
            .windowFunctionConverter(wfc)
            .typeConverter(tc));
  }

  /**
   * Creates a ConverterProvider with full customization including execution behavior.
   *
   * @param typeFactory the Calcite type factory to use
   * @param extensions the Substrait extension collection to use
   * @param sfc the scalar function converter to use
   * @param afc the aggregate function converter to use
   * @param wfc the window function converter to use
   * @param tc the type converter to use
   * @param executionBehavior the execution behavior to use for plans
   * @deprecated Use {@link #builder()} instead; the growing set of components is more readably
   *     configured through the builder than through this positional constructor.
   */
  @Deprecated
  public ConverterProvider(
      RelDataTypeFactory typeFactory,
      SimpleExtension.ExtensionCollection extensions,
      ScalarFunctionConverter sfc,
      AggregateFunctionConverter afc,
      WindowFunctionConverter wfc,
      TypeConverter tc,
      Plan.ExecutionBehavior executionBehavior) {
    this(
        builder()
            .typeFactory(typeFactory)
            .extensions(extensions)
            .scalarFunctionConverter(sfc)
            .aggregateFunctionConverter(afc)
            .windowFunctionConverter(wfc)
            .typeConverter(tc)
            .executionBehavior(executionBehavior));
  }

  /**
   * Master constructor and the sole subclassing seam: derives any unset function converters from
   * the configured extensions and type factory, then assigns all components. {@link
   * Builder#build()} and every delegating public constructor route here, and subclasses (e.g.
   * {@link DynamicConverterProvider}) invoke it via {@code super(builder)}.
   *
   * <p>Taking the {@link Builder} rather than a positional argument list keeps this seam stable as
   * new components are added: a new field is a change to the builder and this constructor only, not
   * to every subclass's {@code super(...)} call.
   *
   * @param builder the builder carrying the configured components
   */
  protected ConverterProvider(Builder builder) {
    this.typeFactory = builder.typeFactory;
    this.extensions = builder.extensions;
    this.scalarFunctionConverter =
        builder.scalarFunctionConverter != null
            ? builder.scalarFunctionConverter
            : new ScalarFunctionConverter(
                builder.extensions.scalarFunctions(), builder.typeFactory);
    this.aggregateFunctionConverter =
        builder.aggregateFunctionConverter != null
            ? builder.aggregateFunctionConverter
            : new AggregateFunctionConverter(
                builder.extensions.aggregateFunctions(), builder.typeFactory);
    this.windowFunctionConverter =
        builder.windowFunctionConverter != null
            ? builder.windowFunctionConverter
            : new WindowFunctionConverter(
                builder.extensions.windowFunctions(), builder.typeFactory);
    this.typeConverter = builder.typeConverter;
    this.executionBehavior = builder.executionBehavior;
    this.sqlParserConfig = builder.sqlParserConfig;
  }

  /**
   * Creates the default execution behavior with PER_PLAN variable evaluation mode.
   *
   * @return the default execution behavior
   */
  private static Plan.ExecutionBehavior createDefaultExecutionBehavior() {
    return ImmutableExecutionBehavior.builder()
        .variableEvaluationMode(Plan.ExecutionBehavior.VariableEvaluationMode.PER_PLAN)
        .build();
  }

  // SQL to Calcite Processing

  /**
   * {@link SqlParser.Config} is a Calcite class which controls SQL parsing behaviour like
   * identifier casing.
   *
   * <p>Defaults to {@link #DEFAULT_SQL_PARSER_CONFIG}. Provide a custom configuration via {@link
   * Builder#sqlParserConfig(SqlParser.Config)} (or the {@link Builder#unquotedCasing(Casing)}
   * convenience), or override this method in a subclass for fully dynamic behaviour.
   *
   * @return the SQL parser configuration
   */
  public SqlParser.Config getSqlParserConfig() {
    return sqlParserConfig;
  }

  /**
   * {@link CalciteConnectionConfig} is a Calcite class which controls SQL processing behaviour like
   * table name case-sensitivity.
   *
   * @return the Calcite connection configuration
   */
  public CalciteConnectionConfig getCalciteConnectionConfig() {
    return CalciteConnectionConfig.DEFAULT.set(CalciteConnectionProperty.CASE_SENSITIVE, "false");
  }

  /**
   * {@link SqlToRelConverter.Config} is a Calcite class which controls SQL processing behaviour
   * like field-trimming.
   *
   * @return the SQL to Rel converter configuration
   */
  public SqlToRelConverter.Config getSqlToRelConverterConfig() {
    return SqlToRelConverter.config().withTrimUnusedFields(true).withExpand(false);
  }

  /**
   * {@link SqlOperatorTable} is a Calcite class which stores the {@link
   * org.apache.calcite.sql.SqlOperator}s available and controls valid identifiers during SQL
   * processing.
   *
   * @return the SQL operator table
   */
  public SqlOperatorTable getSqlOperatorTable() {
    return SubstraitOperatorTable.INSTANCE;
  }

  // Substrait to Calcite Processing

  /**
   * {@link SubstraitToCalcite} is an Isthmus class for converting a Substrait {@link Rel} or {@link
   * io.substrait.plan.Plan.Root} to a Calcite {@link org.apache.calcite.rel.RelNode} or {@link
   * org.apache.calcite.rel.RelRoot}
   *
   * @return a new SubstraitToCalcite converter instance
   */
  protected SubstraitToCalcite getSubstraitToCalcite() {
    return new SubstraitToCalcite(this);
  }

  /**
   * {@link SubstraitToCalcite} is an Isthmus class for converting a Substrait {@link Rel} or {@link
   * io.substrait.plan.Plan.Root} to a Calcite {@link org.apache.calcite.rel.RelNode} or {@link
   * org.apache.calcite.rel.RelRoot}
   *
   * @param catalogReader a Calcite {@link Prepare.CatalogReader} used to construct a {@link
   *     RelBuilder} for use in creating Calcite {@link org.apache.calcite.rel.RelNode}s
   * @return a new SubstraitToCalcite converter instance
   */
  protected SubstraitToCalcite getSubstraitToCalcite(Prepare.CatalogReader catalogReader) {
    return new SubstraitToCalcite(this, catalogReader);
  }

  // Calcite to Substrait Processing

  /**
   * A {@link SubstraitRelVisitor} converts Calcite {@link org.apache.calcite.rel.RelNode}s to
   * Substrait {@link Rel}s
   *
   * @return a new SubstraitRelVisitor instance
   */
  public SubstraitRelVisitor getSubstraitRelVisitor() {
    return new SubstraitRelVisitor(this);
  }

  /**
   * A {@link RexExpressionConverter} converts Calcite {@link org.apache.calcite.rex.RexNode}s to
   * Substrait equivalents.
   *
   * @param srv the SubstraitRelVisitor to use for nested relation conversions
   * @return a new RexExpressionConverter instance
   */
  public RexExpressionConverter getRexExpressionConverter(SubstraitRelVisitor srv) {
    return new RexExpressionConverter(
        srv, getCallConverters(), getWindowFunctionConverter(), getTypeConverter());
  }

  /**
   * {@link CallConverter}s are used to convert Calcite {@link org.apache.calcite.rex.RexCall}s to
   * Substrait equivalents.
   *
   * @return a list of CallConverter instances
   */
  public List<CallConverter> getCallConverters() {
    ArrayList<CallConverter> callConverters = new ArrayList<>();
    callConverters.add(new FieldSelectionConverter(typeConverter));
    callConverters.add(CallConverters.CASE);
    callConverters.add(CallConverters.ROW);
    callConverters.add(CallConverters.CAST.apply(typeConverter));
    callConverters.add(CallConverters.REINTERPRET.apply(typeConverter));
    callConverters.add(CallConverters.EXECUTION_CONTEXT_VARIABLE);
    callConverters.add(new SqlArrayValueConstructorCallConverter(typeConverter));
    callConverters.add(new SqlMapValueConstructorCallConverter());
    callConverters.add(CallConverters.CREATE_SEARCH_CONV.apply(new RexBuilder(typeFactory)));
    callConverters.add(scalarFunctionConverter);
    return callConverters;
  }

  // Substrait To Calcite Processing

  /**
   * When converting from Substrait to Calcite, Calcite needs to have a schema available. The
   * default strategy uses a {@link SchemaCollector} to generate a {@link CalciteSchema} on the fly
   * based on the leaf nodes of a Substrait plan.
   *
   * <p>Override to customize the schema generation behaviour
   *
   * @return a function that resolves a Rel to a CalciteSchema
   */
  public Function<Rel, CalciteSchema> getSchemaResolver() {
    SchemaCollector schemaCollector = new SchemaCollector(this);
    return schemaCollector::toSchema;
  }

  /**
   * A {@link SubstraitRelNodeConverter} is used when converting from Substrait {@link Rel}s to
   * Calcite {@link org.apache.calcite.rel.RelNode}s.
   *
   * @param relBuilder the RelBuilder to use for creating Calcite RelNodes
   * @return a new SubstraitRelNodeConverter instance
   */
  public SubstraitRelNodeConverter getSubstraitRelNodeConverter(RelBuilder relBuilder) {
    return new SubstraitRelNodeConverter(relBuilder, this);
  }

  /**
   * A {@link ExpressionRexConverter} converts Substrait {@link io.substrait.expression.Expression}
   * to Calcite equivalents
   *
   * @param relNodeConverter the SubstraitRelNodeConverter to use for nested relation conversions
   * @return a new ExpressionRexConverter instance
   */
  public ExpressionRexConverter getExpressionRexConverter(
      SubstraitRelNodeConverter relNodeConverter) {
    ExpressionRexConverter erc =
        new ExpressionRexConverter(
            getTypeFactory(),
            getScalarFunctionConverter(),
            getWindowFunctionConverter(),
            getTypeConverter());
    erc.setRelNodeConverter(relNodeConverter);
    return erc;
  }

  /**
   * A {@link RelBuilder} is a Calcite class used for creating {@link
   * org.apache.calcite.rel.RelNode}s.
   *
   * @param schema the CalciteSchema to use as the default schema
   * @return a new RelBuilder instance
   */
  public RelBuilder getRelBuilder(CalciteSchema schema) {
    return RelBuilder.create(
        Frameworks.newConfigBuilder()
            .defaultSchema(schema.plus())
            .typeSystem(getTypeSystem())
            .build());
  }

  // Utility Getters

  /**
   * Returns the Calcite type factory used by this converter provider.
   *
   * @return the type factory
   */
  public RelDataTypeFactory getTypeFactory() {
    return typeFactory;
  }

  /**
   * Returns the Calcite {@link RelDataTypeSystem} used by this converter provider.
   *
   * <p>Derived from the {@link #getTypeFactory() type factory} so the two never disagree. This is
   * the type system supplied to the {@link RelBuilder} used when converting Substrait to Calcite,
   * ensuring its type derivation matches the types carried by converted expressions.
   *
   * @return the type system
   */
  public RelDataTypeSystem getTypeSystem() {
    return typeFactory.getTypeSystem();
  }

  /**
   * Returns the Substrait extension collection used by this converter provider.
   *
   * @return the extension collection
   */
  public SimpleExtension.ExtensionCollection getExtensions() {
    return extensions;
  }

  /**
   * Returns the scalar function converter used by this converter provider.
   *
   * @return the scalar function converter
   */
  public ScalarFunctionConverter getScalarFunctionConverter() {
    return scalarFunctionConverter;
  }

  /**
   * Returns the aggregate function converter used by this converter provider.
   *
   * @return the aggregate function converter
   */
  public AggregateFunctionConverter getAggregateFunctionConverter() {
    return aggregateFunctionConverter;
  }

  /**
   * Returns the window function converter used by this converter provider.
   *
   * @return the window function converter
   */
  public WindowFunctionConverter getWindowFunctionConverter() {
    return windowFunctionConverter;
  }

  /**
   * Returns the type converter used by this converter provider.
   *
   * @return the type converter
   */
  public TypeConverter getTypeConverter() {
    return typeConverter;
  }

  /**
   * Returns the execution behavior for plans created by this converter.
   *
   * <p>The default execution behavior uses {@link
   * Plan.ExecutionBehavior.VariableEvaluationMode#PER_PLAN}, which evaluates variables once per
   * plan execution. This can be customized via {@link
   * Builder#executionBehavior(Plan.ExecutionBehavior)}.
   *
   * @return the execution behavior to use when creating plans
   */
  public Plan.ExecutionBehavior getExecutionBehavior() {
    return executionBehavior;
  }

  /**
   * Creates a new {@link Builder} for configuring a {@link ConverterProvider}.
   *
   * <p>The builder starts from reasonable system defaults (the same ones behind {@link #DEFAULT})
   * and lets callers override individual components — most notably the Calcite {@link
   * SqlParser.Config} used for SQL parsing, via {@link Builder#sqlParserConfig(SqlParser.Config)}
   * for full control or {@link Builder#unquotedCasing(Casing)} for the common casing-only case.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Fluent builder for {@link ConverterProvider}.
   *
   * <p>Unset components fall back to reasonable system defaults. The scalar, aggregate and window
   * function converters, if not set explicitly, are derived from the configured {@link
   * #extensions(SimpleExtension.ExtensionCollection) extensions} and {@link
   * #typeFactory(RelDataTypeFactory) type factory} when {@link #build()} is called.
   */
  public static class Builder {
    private SimpleExtension.ExtensionCollection extensions =
        DefaultExtensionCatalog.DEFAULT_COLLECTION;
    private RelDataTypeFactory typeFactory = SubstraitTypeSystem.TYPE_FACTORY;
    private ScalarFunctionConverter scalarFunctionConverter;
    private AggregateFunctionConverter aggregateFunctionConverter;
    private WindowFunctionConverter windowFunctionConverter;
    private TypeConverter typeConverter = TypeConverter.DEFAULT;
    private Plan.ExecutionBehavior executionBehavior = createDefaultExecutionBehavior();
    private SqlParser.Config sqlParserConfig = DEFAULT_SQL_PARSER_CONFIG;

    /**
     * Sets the Substrait extension collection to use.
     *
     * @param extensions the extension collection
     * @return this builder
     */
    public Builder extensions(SimpleExtension.ExtensionCollection extensions) {
      this.extensions = extensions;
      return this;
    }

    /**
     * Sets the Calcite type factory to use.
     *
     * @param typeFactory the type factory
     * @return this builder
     */
    public Builder typeFactory(RelDataTypeFactory typeFactory) {
      this.typeFactory = typeFactory;
      return this;
    }

    /**
     * Sets the scalar function converter. When left unset, it is derived from the configured
     * extensions and type factory.
     *
     * @param scalarFunctionConverter the scalar function converter
     * @return this builder
     */
    public Builder scalarFunctionConverter(ScalarFunctionConverter scalarFunctionConverter) {
      this.scalarFunctionConverter = scalarFunctionConverter;
      return this;
    }

    /**
     * Sets the aggregate function converter. When left unset, it is derived from the configured
     * extensions and type factory.
     *
     * @param aggregateFunctionConverter the aggregate function converter
     * @return this builder
     */
    public Builder aggregateFunctionConverter(
        AggregateFunctionConverter aggregateFunctionConverter) {
      this.aggregateFunctionConverter = aggregateFunctionConverter;
      return this;
    }

    /**
     * Sets the window function converter. When left unset, it is derived from the configured
     * extensions and type factory.
     *
     * @param windowFunctionConverter the window function converter
     * @return this builder
     */
    public Builder windowFunctionConverter(WindowFunctionConverter windowFunctionConverter) {
      this.windowFunctionConverter = windowFunctionConverter;
      return this;
    }

    /**
     * Sets the type converter.
     *
     * @param typeConverter the type converter
     * @return this builder
     */
    public Builder typeConverter(TypeConverter typeConverter) {
      this.typeConverter = typeConverter;
      return this;
    }

    /**
     * Sets the execution behavior for plans created by the resulting converter.
     *
     * @param executionBehavior the execution behavior
     * @return this builder
     */
    public Builder executionBehavior(Plan.ExecutionBehavior executionBehavior) {
      this.executionBehavior = executionBehavior;
      return this;
    }

    /**
     * Sets the full Calcite {@link SqlParser.Config} used for SQL parsing, replacing the default.
     *
     * <p>Use {@link ConverterProvider#DEFAULT_SQL_PARSER_CONFIG} as a starting point to retain
     * isthmus' DDL parser factory and conformance while overriding individual settings.
     *
     * @param sqlParserConfig the parser configuration
     * @return this builder
     */
    public Builder sqlParserConfig(SqlParser.Config sqlParserConfig) {
      this.sqlParserConfig = sqlParserConfig;
      return this;
    }

    /**
     * Convenience for the common case of overriding only the unquoted-identifier casing, applied on
     * top of the current {@link #sqlParserConfig(SqlParser.Config) parser configuration}.
     *
     * <p>Equivalent to {@code sqlParserConfig(currentConfig.withUnquotedCasing(unquotedCasing))}.
     * Because it layers onto the current configuration, a subsequent {@link
     * #sqlParserConfig(SqlParser.Config)} call replaces the whole configuration and discards the
     * casing set here; set the full config first, then apply this convenience.
     *
     * @param unquotedCasing the casing to apply to unquoted SQL identifiers during parsing
     * @return this builder
     */
    public Builder unquotedCasing(Casing unquotedCasing) {
      this.sqlParserConfig = this.sqlParserConfig.withUnquotedCasing(unquotedCasing);
      return this;
    }

    /**
     * Builds a {@link ConverterProvider} from the configured components, deriving any unset
     * function converters from the configured extensions and type factory.
     *
     * @return a new {@link ConverterProvider}
     */
    public ConverterProvider build() {
      return new ConverterProvider(this);
    }
  }
}
