package io.substrait.isthmus;

import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperator;

public class DynamicConverterProvider extends ConverterProvider {

  private final SimpleExtension.ExtensionCollection extensions;

  private final ScalarFunctionConverter scalarFunctionConverter;

  public DynamicConverterProvider(
      RelDataTypeFactory typeFactory, SimpleExtension.ExtensionCollection extensions) {
    super(typeFactory, extensions);
    this.extensions = extensions;
    this.scalarFunctionConverter = createScalarFunctionConverter(extensions, typeFactory);
  }

  @Override
  protected List<CallConverter> getCallConverters() {
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
            extensions.scalarFunctions(),
            additionalSignatures,
            typeFactory,
            TypeConverter.DEFAULT));
    return callConverters;
  }

  @Override
  public ScalarFunctionConverter getScalarFunctionConverter() {
    return scalarFunctionConverter;
  }

  private static ScalarFunctionConverter createScalarFunctionConverter(
      SimpleExtension.ExtensionCollection extensions, RelDataTypeFactory typeFactory) {

    List<FunctionMappings.Sig> additionalSignatures;

    java.util.Set<String> knownFunctionNames =
        FunctionMappings.SCALAR_SIGS.stream()
            .map(FunctionMappings.Sig::name)
            .collect(Collectors.toSet());

    List<SimpleExtension.ScalarFunctionVariant> dynamicFunctions =
        extensions.scalarFunctions().stream()
            .filter(f -> !knownFunctionNames.contains(f.name().toLowerCase()))
            .collect(Collectors.toList());

    if (dynamicFunctions.isEmpty()) {
      additionalSignatures = Collections.emptyList();
    } else {
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
        extensions.scalarFunctions(), additionalSignatures, typeFactory, TypeConverter.DEFAULT);
  }
}
