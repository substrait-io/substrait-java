package io.substrait.isthmus;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.extension.SimpleExtension.Function;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ConverterProvider that automatically creates dynamic function mappings for unmapped extension
 * functions.
 *
 * <p>This provider identifies functions in the extension collection that don't have explicit
 * mappings in FunctionMappings and automatically generates SqlOperators and function signatures for
 * them. This enables SQL queries to use extension functions without requiring manual mapping
 * configuration.
 *
 * <p>Example use case: Using strftime() from functions_datetime.yaml without adding it to
 * FunctionMappings.SCALAR_SIGS.
 *
 * @see ConverterProvider
 * @see SimpleExtensionToSqlOperator
 */
public class AutomaticDynamicFunctionMappingConverterProvider extends ConverterProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AutomaticDynamicFunctionMappingConverterProvider.class);

  private final SqlOperatorTable operatorTable;

  public AutomaticDynamicFunctionMappingConverterProvider() {
    this(DefaultExtensionCatalog.DEFAULT_COLLECTION, SubstraitTypeSystem.TYPE_FACTORY);
  }

  public AutomaticDynamicFunctionMappingConverterProvider(
      SimpleExtension.ExtensionCollection extensions) {
    this(extensions, SubstraitTypeSystem.TYPE_FACTORY);
  }

  public AutomaticDynamicFunctionMappingConverterProvider(
      SimpleExtension.ExtensionCollection extensions, RelDataTypeFactory typeFactory) {
    super(extensions, typeFactory);

    List<SqlOperator> dynamicScalarOperators = getDynamicScalarOperators();
    this.scalarFunctionConverter = createScalarFunctionConverter(dynamicScalarOperators);

    List<SqlOperator> dynamicAggregateOperators = getDynamicAggregateOperators();
    this.aggregateFunctionConverter = createAggregateFunctionConverter(dynamicAggregateOperators);

    List<SqlOperator> dynamicWindowOperators = getDynamicWindowOperators();
    this.windowFunctionConverter = createWindowFunctionConverter(dynamicWindowOperators);

    List<SqlOperator> allOperators =
        Stream.of(dynamicScalarOperators, dynamicAggregateOperators, dynamicWindowOperators)
            .flatMap(List::stream)
            .collect(Collectors.toList());
    this.operatorTable = buildOperatorTable(allOperators);
  }

  @Override
  public SqlOperatorTable getSqlOperatorTable() {
    return operatorTable;
  }

  private List<SqlOperator> getDynamicScalarOperators() {
    List<SimpleExtension.ScalarFunctionVariant> unmappedFunctions =
        io.substrait.isthmus.expression.FunctionConverter.getUnmappedFunctions(
            extensions.scalarFunctions(), FunctionMappings.SCALAR_SIGS);

    LOGGER.info(
        "Dynamically mapping {} unmapped scalar functions: {}",
        unmappedFunctions.size(),
        unmappedFunctions.stream().map(Function::name).collect(Collectors.toList()));

    return SimpleExtensionToSqlOperator.from(unmappedFunctions, typeFactory);
  }

  private List<SqlOperator> getDynamicAggregateOperators() {
    List<SimpleExtension.AggregateFunctionVariant> unmappedFunctions =
        io.substrait.isthmus.expression.FunctionConverter.getUnmappedFunctions(
            extensions.aggregateFunctions(), FunctionMappings.AGGREGATE_SIGS);

    LOGGER.info(
        "Dynamically mapping {} unmapped aggregate functions: {}",
        unmappedFunctions.size(),
        unmappedFunctions.stream().map(Function::name).collect(Collectors.toList()));

    return SimpleExtensionToSqlOperator.from(unmappedFunctions, typeFactory);
  }

  private List<SqlOperator> getDynamicWindowOperators() {
    List<SimpleExtension.WindowFunctionVariant> unmappedFunctions =
        io.substrait.isthmus.expression.FunctionConverter.getUnmappedFunctions(
            extensions.windowFunctions(), FunctionMappings.WINDOW_SIGS);

    LOGGER.info(
        "Dynamically mapping {} unmapped window functions: {}",
        unmappedFunctions.size(),
        unmappedFunctions.stream().map(Function::name).collect(Collectors.toList()));

    return SimpleExtensionToSqlOperator.from(unmappedFunctions, typeFactory);
  }

  private ScalarFunctionConverter createScalarFunctionConverter(
      List<SqlOperator> dynamicOperators) {
    List<FunctionMappings.Sig> additionalSignatures = createDynamicSignatures(dynamicOperators);
    return new ScalarFunctionConverter(
        extensions.scalarFunctions(), additionalSignatures, typeFactory, typeConverter);
  }

  private AggregateFunctionConverter createAggregateFunctionConverter(
      List<SqlOperator> dynamicOperators) {
    List<FunctionMappings.Sig> additionalSignatures = createDynamicSignatures(dynamicOperators);
    return new AggregateFunctionConverter(
        extensions.aggregateFunctions(), additionalSignatures, typeFactory, typeConverter);
  }

  private WindowFunctionConverter createWindowFunctionConverter(
      List<SqlOperator> dynamicOperators) {
    List<FunctionMappings.Sig> additionalSignatures = createDynamicSignatures(dynamicOperators);
    return new WindowFunctionConverter(
        extensions.windowFunctions(), additionalSignatures, typeFactory, typeConverter);
  }

  private List<FunctionMappings.Sig> createDynamicSignatures(List<SqlOperator> dynamicOperators) {
    Map<String, SqlOperator> uniqueOperators = new LinkedHashMap<>(dynamicOperators.size());
    for (SqlOperator op : dynamicOperators) {
      uniqueOperators.put(op.getName().toLowerCase(Locale.ROOT), op);
    }

    return uniqueOperators.values().stream()
        .map(op -> FunctionMappings.s(op))
        .collect(Collectors.toList());
  }

  private SqlOperatorTable buildOperatorTable(List<SqlOperator> additionalOperators) {
    SqlOperatorTable baseOperatorTable = super.getSqlOperatorTable();

    if (additionalOperators.isEmpty()) {
      return baseOperatorTable;
    }

    return SqlOperatorTables.chain(baseOperatorTable, SqlOperatorTables.of(additionalOperators));
  }
}
