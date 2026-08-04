package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.TypeObserver;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.junit.jupiter.api.Test;

class ConverterProviderBuilderTest {

  static final SimpleExtension.ExtensionCollection EXTENSIONS =
      DefaultExtensionCatalog.DEFAULT_COLLECTION;
  static final RelDataTypeFactory TYPE_FACTORY = SubstraitTypeSystem.TYPE_FACTORY;

  @Test
  void derivesFunctionConvertersWhenUnset() {
    ConverterProvider provider = ConverterProvider.builder().build();

    assertNotNull(provider.getScalarFunctionConverter());
    assertNotNull(provider.getAggregateFunctionConverter());
    assertNotNull(provider.getWindowFunctionConverter());
    assertEquals(ConverterProvider.DEFAULT_SQL_PARSER_CONFIG, provider.getSqlParserConfig());
    assertSame(TypeObserver.NOOP, provider.getTypeObserver());
  }

  @Test
  void usesExplicitlyConfiguredFunctionConverters() {
    ScalarFunctionConverter sfc = scalarFunctionConverter();
    AggregateFunctionConverter afc = aggregateFunctionConverter();
    WindowFunctionConverter wfc = windowFunctionConverter();

    ConverterProvider provider =
        ConverterProvider.builder()
            .scalarFunctionConverter(sfc)
            .aggregateFunctionConverter(afc)
            .windowFunctionConverter(wfc)
            .build();

    assertSame(sfc, provider.getScalarFunctionConverter());
    assertSame(afc, provider.getAggregateFunctionConverter());
    assertSame(wfc, provider.getWindowFunctionConverter());
  }

  @Test
  void dynamicConverterProviderRejectsConfiguredScalarFunctionConverter() {
    ConverterProvider.Builder builder =
        ConverterProvider.builder().scalarFunctionConverter(scalarFunctionConverter());

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> new DynamicConverterProvider(builder));
    assertTrue(e.getMessage().contains("scalarFunctionConverter"), e.getMessage());
  }

  @Test
  void dynamicConverterProviderAcceptsBuilderWithoutFunctionConverters() {
    assertDoesNotThrow(
        () -> new DynamicConverterProvider(ConverterProvider.builder().extensions(EXTENSIONS)));
  }

  @Test
  void automaticDynamicProviderRejectsConfiguredScalarFunctionConverter() {
    assertRejected(
        ConverterProvider.builder().scalarFunctionConverter(scalarFunctionConverter()),
        "scalarFunctionConverter");
  }

  @Test
  void automaticDynamicProviderRejectsConfiguredAggregateFunctionConverter() {
    assertRejected(
        ConverterProvider.builder().aggregateFunctionConverter(aggregateFunctionConverter()),
        "aggregateFunctionConverter");
  }

  @Test
  void automaticDynamicProviderRejectsConfiguredWindowFunctionConverter() {
    assertRejected(
        ConverterProvider.builder().windowFunctionConverter(windowFunctionConverter()),
        "windowFunctionConverter");
  }

  @Test
  void automaticDynamicProviderAcceptsBuilderWithoutFunctionConverters() {
    assertDoesNotThrow(
        () ->
            new AutomaticDynamicFunctionMappingConverterProvider(
                ConverterProvider.builder().extensions(EXTENSIONS)));
  }

  private static void assertRejected(ConverterProvider.Builder builder, String setterName) {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> new AutomaticDynamicFunctionMappingConverterProvider(builder));
    assertTrue(e.getMessage().contains(setterName), e.getMessage());
  }

  private static ScalarFunctionConverter scalarFunctionConverter() {
    return new ScalarFunctionConverter(EXTENSIONS.scalarFunctions(), TYPE_FACTORY);
  }

  private static AggregateFunctionConverter aggregateFunctionConverter() {
    return new AggregateFunctionConverter(EXTENSIONS.aggregateFunctions(), TYPE_FACTORY);
  }

  private static WindowFunctionConverter windowFunctionConverter() {
    return new WindowFunctionConverter(EXTENSIONS.windowFunctions(), TYPE_FACTORY);
  }
}
