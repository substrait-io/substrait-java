package io.substrait.isthmus;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.calcite.SubstraitOperatorTable;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.CallConverters;
import io.substrait.isthmus.expression.FieldSelectionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.SqlArrayValueConstructorCallConverter;
import io.substrait.isthmus.expression.SqlMapValueConstructorCallConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.tools.RelBuilder;

public class ConverterProvider {

  protected final RelDataTypeFactory typeFactory;

  private final ScalarFunctionConverter scalarFunctionConverter;
  private final AggregateFunctionConverter aggregateFunctionConverter;
  private final WindowFunctionConverter windowFunctionConverter;

  private final TypeConverter typeConverter;

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

  protected List<CallConverter> getCallConverters() {
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

  protected SqlOperatorTable getSqlOperatorTable() {
    return SubstraitOperatorTable.INSTANCE;
  }

  protected SubstraitRelNodeConverter getSubstraitRelNodeConverter(RelBuilder relBuilder) {
    return new SubstraitRelNodeConverter(
        typeFactory,
        relBuilder,
        getScalarFunctionConverter(),
        getAggregateFunctionConverter(),
        getWindowFunctionConverter(),
        typeConverter);
  }

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
