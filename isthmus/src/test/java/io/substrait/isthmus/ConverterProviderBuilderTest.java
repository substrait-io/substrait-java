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
import io.substrait.isthmus.expression.WindowFunctionConverter;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.Nested;
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

  @Nested
  class UnquotedCasing {

    @Test
    void defaultsToUpper() {
      ConverterProvider provider = ConverterProvider.builder().build();
      assertEquals(Casing.TO_UPPER, provider.getSqlParserConfig().unquotedCasing());
    }

    @Test
    void configuredCasingIsUsed() {
      ConverterProvider provider =
          ConverterProvider.builder().unquotedCasing(Casing.UNCHANGED).build();
      assertEquals(Casing.UNCHANGED, provider.getSqlParserConfig().unquotedCasing());
    }

    /**
     * A full {@link SqlParser.Config} supplied to the builder is used verbatim. Deriving it from
     * {@link ConverterProvider#DEFAULT_SQL_PARSER_CONFIG} preserves Isthmus' parser defaults (here,
     * {@link SqlConformanceEnum#LENIENT} conformance) while overriding a single setting.
     */
    @Test
    void fullSqlParserConfigIsUsed() {
      SqlParser.Config config =
          ConverterProvider.DEFAULT_SQL_PARSER_CONFIG.withUnquotedCasing(Casing.TO_LOWER);
      ConverterProvider provider = ConverterProvider.builder().sqlParserConfig(config).build();
      assertEquals(Casing.TO_LOWER, provider.getSqlParserConfig().unquotedCasing());
      assertEquals(SqlConformanceEnum.LENIENT, provider.getSqlParserConfig().conformance());
    }

    /**
     * The casing is applied over the configured {@link SqlParser.Config} at construction, so it
     * wins over the casing that config carries no matter which order the two setters are called in,
     * and the rest of the supplied config is retained either way.
     */
    @Test
    void isOrderIndependentWithSqlParserConfig() {
      SqlParser.Config config =
          ConverterProvider.DEFAULT_SQL_PARSER_CONFIG
              .withUnquotedCasing(Casing.TO_LOWER)
              .withConformance(SqlConformanceEnum.PRAGMATIC_2003);

      ConverterProvider casingLast =
          ConverterProvider.builder()
              .sqlParserConfig(config)
              .unquotedCasing(Casing.UNCHANGED)
              .build();
      ConverterProvider casingFirst =
          ConverterProvider.builder()
              .unquotedCasing(Casing.UNCHANGED)
              .sqlParserConfig(config)
              .build();

      assertEquals(Casing.UNCHANGED, casingLast.getSqlParserConfig().unquotedCasing());
      assertEquals(Casing.UNCHANGED, casingFirst.getSqlParserConfig().unquotedCasing());
      assertEquals(
          SqlConformanceEnum.PRAGMATIC_2003, casingLast.getSqlParserConfig().conformance());
      assertEquals(
          SqlConformanceEnum.PRAGMATIC_2003, casingFirst.getSqlParserConfig().conformance());
    }
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
