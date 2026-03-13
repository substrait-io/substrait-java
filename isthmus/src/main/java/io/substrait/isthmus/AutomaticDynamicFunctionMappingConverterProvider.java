package io.substrait.isthmus;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
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

  private SqlOperatorTable cachedOperatorTable = null;
  private final List<SqlOperator> additionalOperators = new ArrayList<>();

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
    this.scalarFunctionConverter = createScalarFunctionConverter();
    this.aggregateFunctionConverter = createAggregateFunctionConverter();
    this.windowFunctionConverter = createWindowFunctionConverter();
  }

  @Override
  public SqlOperatorTable getSqlOperatorTable() {
    if (cachedOperatorTable == null) {
      cachedOperatorTable = buildOperatorTable();
    }
    return cachedOperatorTable;
  }

  private SqlOperatorTable buildOperatorTable() {
    SqlOperatorTable baseOperatorTable = super.getSqlOperatorTable();

    if (!additionalOperators.isEmpty()) {
      return SqlOperatorTables.chain(baseOperatorTable, SqlOperatorTables.of(additionalOperators));
    } else {
      return baseOperatorTable;
    }
  }

  private List<FunctionMappings.Sig> createDynamicSignatures(
      String functionType, List<? extends SimpleExtension.Function> unmappedFunctions) {
    if (unmappedFunctions.isEmpty()) {
      return java.util.Collections.emptyList();
    }

    LOGGER.info(
        "Dynamically mapping {} unmapped {} functions: {}",
        unmappedFunctions.size(),
        functionType,
        unmappedFunctions.stream().map(f -> f.name()).collect(Collectors.toList()));

    List<SqlOperator> dynamicOperators =
        SimpleExtensionToSqlOperator.from(unmappedFunctions, typeFactory);

    this.additionalOperators.addAll(dynamicOperators);

    java.util.Map<String, SqlOperator> operatorsByName = new java.util.LinkedHashMap<>();
    for (SqlOperator op : dynamicOperators) {
      operatorsByName.put(op.getName().toLowerCase(), op);
    }

    return operatorsByName.values().stream()
        .map(op -> FunctionMappings.s(op, op.getName().toLowerCase()))
        .collect(Collectors.toList());
  }

  protected ScalarFunctionConverter createScalarFunctionConverter() {
    List<SimpleExtension.ScalarFunctionVariant> unmappedFunctions =
        io.substrait.isthmus.expression.FunctionConverter.getUnmappedFunctions(
            extensions.scalarFunctions(), FunctionMappings.SCALAR_SIGS);

    List<FunctionMappings.Sig> additionalSignatures =
        new ArrayList<>(createDynamicSignatures("scalar", unmappedFunctions));

    return new ScalarFunctionConverter(
        extensions.scalarFunctions(), additionalSignatures, typeFactory, typeConverter);
  }

  protected AggregateFunctionConverter createAggregateFunctionConverter() {
    List<SimpleExtension.AggregateFunctionVariant> unmappedFunctions =
        io.substrait.isthmus.expression.FunctionConverter.getUnmappedFunctions(
            extensions.aggregateFunctions(), FunctionMappings.AGGREGATE_SIGS);

    List<FunctionMappings.Sig> additionalSignatures =
        new ArrayList<>(createDynamicSignatures("aggregate", unmappedFunctions));

    return new AggregateFunctionConverter(
        extensions.aggregateFunctions(), additionalSignatures, typeFactory, typeConverter);
  }

  protected WindowFunctionConverter createWindowFunctionConverter() {
    List<SimpleExtension.WindowFunctionVariant> unmappedFunctions =
        io.substrait.isthmus.expression.FunctionConverter.getUnmappedFunctions(
            extensions.windowFunctions(), FunctionMappings.WINDOW_SIGS);

    List<FunctionMappings.Sig> additionalSignatures =
        new ArrayList<>(createDynamicSignatures("window", unmappedFunctions));

    return new WindowFunctionConverter(
        extensions.windowFunctions(), additionalSignatures, typeFactory, typeConverter);
  }
}
