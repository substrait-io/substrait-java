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

public class AutomaticDynamicFunctionMappingConverterProvider extends ConverterProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AutomaticDynamicFunctionMappingConverterProvider.class);

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
    SqlOperatorTable baseOperatorTable = super.getSqlOperatorTable();
    List<SqlOperator> dynamicOperators = new ArrayList<>();

    List<SimpleExtension.ScalarFunctionVariant> unmappedScalars =
        io.substrait.isthmus.expression.FunctionConverter.getUnmappedFunctions(
            extensions.scalarFunctions(),
            io.substrait.isthmus.expression.FunctionMappings.SCALAR_SIGS);
    List<SimpleExtension.AggregateFunctionVariant> unmappedAggregates =
        io.substrait.isthmus.expression.FunctionConverter.getUnmappedFunctions(
            extensions.aggregateFunctions(),
            io.substrait.isthmus.expression.FunctionMappings.AGGREGATE_SIGS);
    List<SimpleExtension.WindowFunctionVariant> unmappedWindows =
        io.substrait.isthmus.expression.FunctionConverter.getUnmappedFunctions(
            extensions.windowFunctions(),
            io.substrait.isthmus.expression.FunctionMappings.WINDOW_SIGS);

    if (!unmappedScalars.isEmpty()) {
      dynamicOperators.addAll(SimpleExtensionToSqlOperator.from(unmappedScalars, typeFactory));
    }
    if (!unmappedAggregates.isEmpty()) {
      dynamicOperators.addAll(SimpleExtensionToSqlOperator.from(unmappedAggregates, typeFactory));
    }
    if (!unmappedWindows.isEmpty()) {
      dynamicOperators.addAll(SimpleExtensionToSqlOperator.from(unmappedWindows, typeFactory));
    }

    if (!dynamicOperators.isEmpty()) {
      return SqlOperatorTables.chain(baseOperatorTable, SqlOperatorTables.of(dynamicOperators));
    } else {
      return baseOperatorTable;
    }
  }

  protected ScalarFunctionConverter createScalarFunctionConverter() {
    List<SimpleExtension.ScalarFunctionVariant> unmappedFunctions =
        io.substrait.isthmus.expression.FunctionConverter.getUnmappedFunctions(
            extensions.scalarFunctions(), FunctionMappings.SCALAR_SIGS);

    List<FunctionMappings.Sig> additionalSignatures = new ArrayList<>();

    if (!unmappedFunctions.isEmpty()) {
      LOGGER.info(
          "Dynamically mapping {} unmapped scalar functions: {}",
          unmappedFunctions.size(),
          unmappedFunctions.stream().map(f -> f.name()).collect(Collectors.toList()));

      List<SqlOperator> dynamicOperators =
          SimpleExtensionToSqlOperator.from(unmappedFunctions, typeFactory);

      java.util.Map<String, SqlOperator> operatorsByName = new java.util.LinkedHashMap<>();
      for (SqlOperator op : dynamicOperators) {
        operatorsByName.put(op.getName().toLowerCase(), op);
      }

      additionalSignatures.addAll(
          operatorsByName.values().stream()
              .map(op -> FunctionMappings.s(op, op.getName().toLowerCase()))
              .collect(Collectors.toList()));
    }

    return new ScalarFunctionConverter(
        extensions.scalarFunctions(), additionalSignatures, typeFactory, typeConverter);
  }

  protected AggregateFunctionConverter createAggregateFunctionConverter() {
    List<FunctionMappings.Sig> additionalSignatures = new ArrayList<>();

    List<SimpleExtension.AggregateFunctionVariant> unmappedFunctions =
        io.substrait.isthmus.expression.FunctionConverter.getUnmappedFunctions(
            extensions.aggregateFunctions(), FunctionMappings.AGGREGATE_SIGS);

    if (!unmappedFunctions.isEmpty()) {
      List<SqlOperator> dynamicOperators =
          SimpleExtensionToSqlOperator.from(unmappedFunctions, typeFactory);

      java.util.Map<String, SqlOperator> operatorsByName = new java.util.LinkedHashMap<>();
      for (SqlOperator op : dynamicOperators) {
        operatorsByName.put(op.getName().toLowerCase(), op);
      }

      additionalSignatures.addAll(
          operatorsByName.values().stream()
              .map(op -> FunctionMappings.s(op, op.getName().toLowerCase()))
              .collect(Collectors.toList()));
    }

    return new AggregateFunctionConverter(
        extensions.aggregateFunctions(), additionalSignatures, typeFactory, typeConverter);
  }

  protected WindowFunctionConverter createWindowFunctionConverter() {
    List<FunctionMappings.Sig> additionalSignatures = new ArrayList<>();

    List<SimpleExtension.WindowFunctionVariant> unmappedFunctions =
        io.substrait.isthmus.expression.FunctionConverter.getUnmappedFunctions(
            extensions.windowFunctions(), FunctionMappings.WINDOW_SIGS);

    if (!unmappedFunctions.isEmpty()) {
      List<SqlOperator> dynamicOperators =
          SimpleExtensionToSqlOperator.from(unmappedFunctions, typeFactory);

      java.util.Map<String, SqlOperator> operatorsByName = new java.util.LinkedHashMap<>();
      for (SqlOperator op : dynamicOperators) {
        operatorsByName.put(op.getName().toLowerCase(), op);
      }

      additionalSignatures.addAll(
          operatorsByName.values().stream()
              .map(op -> FunctionMappings.s(op, op.getName().toLowerCase()))
              .collect(Collectors.toList()));
    }

    return new WindowFunctionConverter(
        extensions.windowFunctions(), additionalSignatures, typeFactory, typeConverter);
  }
}
