package io.substrait.isthmus;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;

/**
 * A converter provider that dynamically generates SQL operators and call converters from Substrait
 * extension functions.
 *
 * <p>This class extends {@link ConverterProvider} to support dynamic function extensions that are
 * not part of the standard Substrait function set. It automatically generates SQL operators for
 * dynamic extensions and creates appropriate call converters to handle them during query
 * translation.
 *
 * <p>A dynamic extension is a user-defined function (UDF) that is not part of the standard
 * Substrait function catalog. These are custom functions that users define and provide at runtime,
 * extending the built-in function set with domain-specific or application-specific operations.
 *
 * <p>Dynamic extensions are identified as functions that are not present in the known function
 * mappings ({@link FunctionMappings#SCALAR_SIGS}). These functions are then converted to SQL
 * operators and integrated into the operator table and call converter list.
 */
public class DynamicConverterProvider extends ConverterProvider {

  /**
   * Creates a new DynamicConverterProvider with default extension catalog and type factory.
   *
   * <p>Uses {@link DefaultExtensionCatalog#DEFAULT_COLLECTION} for extensions and {@link
   * SubstraitTypeSystem#TYPE_FACTORY} for type operations.
   */
  public DynamicConverterProvider() {
    this(DefaultExtensionCatalog.DEFAULT_COLLECTION, SubstraitTypeSystem.TYPE_FACTORY);
  }

  /**
   * Creates a new DynamicConverterProvider with the specified extension collection.
   *
   * <p>Uses {@link SubstraitTypeSystem#TYPE_FACTORY} for type operations.
   *
   * @param extensions the collection of Substrait extensions to use for function mappings
   */
  public DynamicConverterProvider(SimpleExtension.ExtensionCollection extensions) {
    this(extensions, SubstraitTypeSystem.TYPE_FACTORY);
  }

  /**
   * Creates a new DynamicConverterProvider with the specified extension collection and type
   * factory.
   *
   * <p>This constructor initializes the converter provider with custom extensions and a custom type
   * factory, allowing for full control over function mappings and type conversions.
   *
   * @param extensions the collection of Substrait extensions to use for function mappings
   * @param typeFactory the factory to use for creating and managing relational data types
   */
  public DynamicConverterProvider(
      SimpleExtension.ExtensionCollection extensions, RelDataTypeFactory typeFactory) {
    super(extensions, typeFactory);
    this.scalarFunctionConverter = createScalarFunctionConverter();
  }

  /**
   * Returns the list of call converters, including dynamically generated converters for extension
   * functions.
   *
   * <p>This method extends the base call converters with additional converters for dynamic
   * extensions. It identifies dynamic extensions, generates SQL operators for them, and creates a
   * {@link ScalarFunctionConverter} to handle these functions during query translation.
   *
   * @return a list of call converters including both standard and dynamically generated converters
   */
  @Override
  public List<CallConverter> getCallConverters() {
    List<CallConverter> callConverters = super.getCallConverters();

    SimpleExtension.ExtensionCollection dynamicExtensionCollection =
        ExtensionUtils.getDynamicExtensions(extensions);
    List<SqlOperator> dynamicOperators =
        SimpleExtensionToSqlOperator.from(dynamicExtensionCollection, typeFactory);
    List<FunctionMappings.Sig> additionalSignatures =
        dynamicOperators.stream()
            .map(op -> FunctionMappings.s(op, op.getName()))
            .collect(Collectors.toList());
    callConverters.add(
        new ScalarFunctionConverter(
            extensions.scalarFunctions(), additionalSignatures, typeFactory, typeConverter));
    return callConverters;
  }

  /**
   * Returns the SQL operator table, including dynamically generated operators for extension
   * functions.
   *
   * <p>This method extends the base operator table with operators generated from dynamic
   * extensions. If no dynamic scalar or aggregate functions are present, it returns the base
   * operator table unchanged. Otherwise, it chains the base table with a new table containing the
   * dynamically generated operators.
   *
   * @return a SQL operator table containing both standard and dynamically generated operators
   */
  @Override
  public SqlOperatorTable getSqlOperatorTable() {
    SqlOperatorTable operatorTable = super.getSqlOperatorTable();
    SimpleExtension.ExtensionCollection dynamicExtensionCollection =
        ExtensionUtils.getDynamicExtensions(extensions);
    if (dynamicExtensionCollection.scalarFunctions().isEmpty()
        && dynamicExtensionCollection.aggregateFunctions().isEmpty()) {
      return operatorTable;
    }
    List<SqlOperator> generatedDynamicOperators =
        SimpleExtensionToSqlOperator.from(dynamicExtensionCollection, typeFactory);
    return SqlOperatorTables.chain(operatorTable, SqlOperatorTables.of(generatedDynamicOperators));
  }

  /**
   * Creates a scalar function converter with support for dynamic extension functions.
   *
   * <p>This method identifies scalar functions from the extension collection that are not part of
   * the known function mappings ({@link FunctionMappings#SCALAR_SIGS}). For these dynamic
   * functions, it generates SQL operators and creates function signatures that can be used by the
   * {@link ScalarFunctionConverter}.
   *
   * @return a scalar function converter configured with both standard and dynamic function
   *     signatures
   */
  private ScalarFunctionConverter createScalarFunctionConverter() {
    List<FunctionMappings.Sig> additionalSignatures = Collections.emptyList();

    java.util.Set<String> knownFunctionNames =
        FunctionMappings.SCALAR_SIGS.stream()
            .map(FunctionMappings.Sig::name)
            .collect(Collectors.toSet());

    List<SimpleExtension.ScalarFunctionVariant> dynamicFunctions =
        extensions.scalarFunctions().stream()
            .filter(f -> !knownFunctionNames.contains(f.name().toLowerCase()))
            .collect(Collectors.toList());

    if (!dynamicFunctions.isEmpty()) {
      SimpleExtension.ExtensionCollection dynamicExtensionCollection =
          SimpleExtension.ExtensionCollection.builder().scalarFunctions(dynamicFunctions).build();

      List<SqlOperator> dynamicOperators =
          SimpleExtensionToSqlOperator.from(dynamicExtensionCollection, typeFactory);

      additionalSignatures =
          dynamicOperators.stream()
              .map(op -> FunctionMappings.s(op, op.getName()))
              .collect(Collectors.toList());
    }

    return new ScalarFunctionConverter(
        extensions.scalarFunctions(), additionalSignatures, typeFactory, typeConverter);
  }
}
