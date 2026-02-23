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

public class DynamicConverterProvider extends ConverterProvider {

  public DynamicConverterProvider() {
    this(DefaultExtensionCatalog.DEFAULT_COLLECTION, SubstraitTypeSystem.TYPE_FACTORY);
  }

  public DynamicConverterProvider(SimpleExtension.ExtensionCollection extensions) {
    this(extensions, SubstraitTypeSystem.TYPE_FACTORY);
  }

  public DynamicConverterProvider(
      SimpleExtension.ExtensionCollection extensions, RelDataTypeFactory typeFactory) {
    super(extensions, typeFactory);
    this.scalarFunctionConverter = createScalarFunctionConverter();
  }

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
