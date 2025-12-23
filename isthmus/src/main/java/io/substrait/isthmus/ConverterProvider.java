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

  protected RelDataTypeFactory typeFactory;

  protected ScalarFunctionConverter scalarFunctionConverter;
  protected AggregateFunctionConverter aggregateFunctionConverter;
  protected WindowFunctionConverter windowFunctionConverter;

  protected TypeConverter typeConverter;

  public ConverterProvider() {
    this(SubstraitTypeSystem.TYPE_FACTORY, DefaultExtensionCatalog.DEFAULT_COLLECTION);
  }

  public ConverterProvider(SimpleExtension.ExtensionCollection extensions) {
    this(SubstraitTypeSystem.TYPE_FACTORY, extensions);
  }

  public ConverterProvider(
      RelDataTypeFactory typeFactory, SimpleExtension.ExtensionCollection extensions) {
    this(
        typeFactory,
        new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory),
        new AggregateFunctionConverter(extensions.aggregateFunctions(), typeFactory),
        new WindowFunctionConverter(extensions.windowFunctions(), typeFactory),
        TypeConverter.DEFAULT);
  }

  public ConverterProvider(
      RelDataTypeFactory typeFactory,
      ScalarFunctionConverter sfc,
      AggregateFunctionConverter afc,
      WindowFunctionConverter wfc,
      TypeConverter tc) {
    this.typeFactory = typeFactory;
    this.scalarFunctionConverter = sfc;
    this.aggregateFunctionConverter = afc;
    this.windowFunctionConverter = wfc;
    this.typeConverter = tc;
  }

  // SQL to Calcite Processing

  /**
   * A {@link SqlParser.Config} is a Calcite class which controls SQL parsing behaviour like
   * identifier casing.
   */
  public SqlParser.Config getSqlParserConfig() {
    return SqlParser.Config.DEFAULT
        .withUnquotedCasing(Casing.TO_UPPER)
        .withParserFactory(SqlDdlParserImpl.FACTORY)
        .withConformance(SqlConformanceEnum.LENIENT);
  }

  /**
   * A {@link CalciteConnectionConfig} is a Calcite class which controls SQL processing behaviour
   * like table name case-sensitivity.
   */
  public CalciteConnectionConfig getCalciteConnectionConfig() {
    return CalciteConnectionConfig.DEFAULT.set(CalciteConnectionProperty.CASE_SENSITIVE, "false");
  }

  /**
   * A {@link SqlToRelConverter.Config} is a Calcite class which controls SQL processing behaviour
   * like field-trimming.
   */
  public SqlToRelConverter.Config getSqlToRelConverterConfig() {
    return SqlToRelConverter.config().withTrimUnusedFields(true).withExpand(false);
  }

  /**
   * A {@link SqlOperatorTable} is a Calcite class which stores the {@link
   * org.apache.calcite.sql.SqlOperator}s available and controls valid identifiers during SQL
   * processing.
   */
  public SqlOperatorTable getSqlOperatorTable() {
    return SubstraitOperatorTable.INSTANCE;
  }

  // Calcite to Substrait Processing

  /**
   * A {@link SubstraitRelVisitor} converts Calcite {@link org.apache.calcite.rel.RelNode}s to
   * Substrait {@link Rel}s
   */
  public SubstraitRelVisitor getSubstraitRelVisitor() {
    return new SubstraitRelVisitor(this);
  }

  /**
   * A {@link RexExpressionConverter} converts Calcite {@link org.apache.calcite.rex.RexNode}s to
   * Substrait equivalents.
   */
  public RexExpressionConverter getRexExpressionConverter(SubstraitRelVisitor srv) {
    return new RexExpressionConverter(
        srv, getCallConverters(), getWindowFunctionConverter(), getTypeConverter());
  }

  /**
   * {@link CallConverter}s are used to convert Calcite {@link org.apache.calcite.rex.RexCall}s to
   * Substrait equivalents.
   */
  public List<CallConverter> getCallConverters() {
    ArrayList<CallConverter> callConverters = new ArrayList<>();
    callConverters.add(new FieldSelectionConverter(typeConverter));
    callConverters.add(CallConverters.CASE);
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
   */
  public Function<Rel, CalciteSchema> getSchemaResolver() {
    SchemaCollector schemaCollector = new SchemaCollector(this);
    return schemaCollector::toSchema;
  }

  /**
   * A {@link SubstraitRelNodeConverter} is used when converting from Substrait {@link Rel}s to
   * Calcite {@link org.apache.calcite.rel.RelNode}s.
   */
  public SubstraitRelNodeConverter getSubstraitRelNodeConverter(RelBuilder relBuilder) {
    return new SubstraitRelNodeConverter(relBuilder, this);
  }

  /**
   * A {@link ExpressionRexConverter} converts Substrait {@link io.substrait.expression.Expression}
   * to Calcite equivalents
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
   * A {@link RelBuilder} is a Calcite class used as a factory for creating {@link
   * org.apache.calcite.rel.RelNode}s.
   */
  public RelBuilder getRelBuilder(CalciteSchema schema) {
    return RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(schema.plus()).build());
  }

  // Utility Getters

  public RelDataTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public ScalarFunctionConverter getScalarFunctionConverter() {
    return scalarFunctionConverter;
  }

  public AggregateFunctionConverter getAggregateFunctionConverter() {
    return aggregateFunctionConverter;
  }

  public WindowFunctionConverter getWindowFunctionConverter() {
    return windowFunctionConverter;
  }

  public TypeConverter getTypeConverter() {
    return typeConverter;
  }
}
