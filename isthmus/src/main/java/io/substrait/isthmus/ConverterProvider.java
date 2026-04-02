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
 * <p>The no argument constructor {@link #ConverterProvider()} provides reasonable system defaults.
 *
 * <p>Other constructors allow for further customization of conversion behaviours.
 *
 * <p>More in-depth customization can be achieved by extending this class, as is done in {@link
 * DynamicConverterProvider}.
 */
public class ConverterProvider {

  /** The Calcite type factory used for creating and managing data types. */
  protected RelDataTypeFactory typeFactory;

  /** The collection of Substrait extensions (functions and types) available for conversion. */
  protected final SimpleExtension.ExtensionCollection extensions;

  /** Converter for Substrait scalar functions. */
  protected ScalarFunctionConverter scalarFunctionConverter;

  /** Converter for Substrait aggregate functions. */
  protected AggregateFunctionConverter aggregateFunctionConverter;

  /** Converter for Substrait window functions. */
  protected WindowFunctionConverter windowFunctionConverter;

  /** Converter for Substrait types to Calcite types and vice versa. */
  protected TypeConverter typeConverter;

  /**
   * Creates a ConverterProvider with default extension collection and type factory. Uses {@link
   * DefaultExtensionCatalog#DEFAULT_COLLECTION} and {@link SubstraitTypeSystem#TYPE_FACTORY}.
   */
  public ConverterProvider() {
    this(DefaultExtensionCatalog.DEFAULT_COLLECTION, SubstraitTypeSystem.TYPE_FACTORY);
  }

  /**
   * Creates a ConverterProvider with the specified extension collection and default type factory.
   *
   * @param extensions the Substrait extension collection to use
   */
  public ConverterProvider(SimpleExtension.ExtensionCollection extensions) {
    this(extensions, SubstraitTypeSystem.TYPE_FACTORY);
  }

  /**
   * Creates a ConverterProvider with the specified extension collection and type factory.
   *
   * @param extensions the Substrait extension collection to use
   * @param typeFactory the Calcite type factory to use
   */
  public ConverterProvider(
      SimpleExtension.ExtensionCollection extensions, RelDataTypeFactory typeFactory) {
    this(
        typeFactory,
        extensions,
        new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory),
        new AggregateFunctionConverter(extensions.aggregateFunctions(), typeFactory),
        new WindowFunctionConverter(extensions.windowFunctions(), typeFactory),
        TypeConverter.DEFAULT);
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
   */
  public ConverterProvider(
      RelDataTypeFactory typeFactory,
      SimpleExtension.ExtensionCollection extensions,
      ScalarFunctionConverter sfc,
      AggregateFunctionConverter afc,
      WindowFunctionConverter wfc,
      TypeConverter tc) {
    this.typeFactory = typeFactory;
    this.extensions = extensions;
    this.scalarFunctionConverter = sfc;
    this.aggregateFunctionConverter = afc;
    this.windowFunctionConverter = wfc;
    this.typeConverter = tc;
  }

  // SQL to Calcite Processing

  /**
   * {@link SqlParser.Config} is a Calcite class which controls SQL parsing behaviour like
   * identifier casing.
   *
   * @return the SQL parser configuration
   */
  public SqlParser.Config getSqlParserConfig() {
    return SqlParser.Config.DEFAULT
        .withUnquotedCasing(Casing.TO_UPPER)
        .withParserFactory(SqlDdlParserImpl.FACTORY)
        .withConformance(SqlConformanceEnum.LENIENT);
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
    return RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(schema.plus()).build());
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
}
