package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.TypeObserver;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.RelBuilder;
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

  @Nested
  class CallConverterTransform {

    /** A function no built-in {@link CallConverter} claims. */
    private final SqlFunction unclaimed =
        new SqlFunction(
            "UNCLAIMED",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BIGINT,
            null,
            OperandTypes.NILADIC,
            SqlFunctionCategory.USER_DEFINED_FUNCTION);

    private final RexBuilder rex = new RexBuilder(TYPE_FACTORY);

    private RexNode unclaimedCall() {
      return rex.makeCall(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), unclaimed, List.of());
    }

    private RexNode castCall() {
      return rex.makeAbstractCast(
          TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
          rex.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), 0));
    }

    /**
     * Converts through the provider's own {@link RexExpressionConverter}, which is what every
     * conversion path uses, rather than assembling one out of the provider's parts: whether the
     * added converters reach a conversion at all is what these tests are about.
     */
    private Expression convert(ConverterProvider provider, RexNode node) {
      return node.accept(provider.getRexExpressionConverter(null));
    }

    /**
     * A windowed call goes to the window function converter, but the expressions inside it do not:
     * its operands, partition keys and sort keys are converted like any others, so a caller's
     * converter is consulted for them.
     */
    @Test
    void areConsultedForTheExpressionsInsideAWindowedCall() {
      List<String> seen = new ArrayList<>();
      ConverterProvider provider =
          ConverterProvider.builder()
              .callConverters(
                  prepending(
                      (call, top) -> {
                        seen.add(call.getOperator().getName());
                        return Optional.empty();
                      }))
              .build();
      RelBuilder relBuilder = new RelCreator().createRelBuilder();
      relBuilder.values(new String[] {"a"}, 1, 2);
      RexNode over =
          relBuilder
              .aggregateCall(SqlStdOperatorTable.ROW_NUMBER)
              .over()
              .partitionBy(
                  relBuilder.call(
                      SqlStdOperatorTable.PLUS, relBuilder.field("a"), relBuilder.literal(1)))
              .orderBy(
                  relBuilder.call(
                      SqlStdOperatorTable.MINUS, relBuilder.field("a"), relBuilder.literal(2)))
              .rowsUnbounded()
              .toRex();

      over.accept(provider.getRexExpressionConverter(null));

      assertEquals(List.of("+", "-"), seen);
    }

    /**
     * A subclass appends to what {@code super.getCallConverters()} returns -- which is now the list
     * the transform handed back -- so the transform has to leave it mutable.
     */
    @Test
    void leaveTheListMutableForASubclassToAppendTo() {
      ConverterProvider provider =
          ConverterProvider.builder()
              .callConverters(prepending(claiming(call -> false, null)))
              .build();

      List<CallConverter> converters = provider.getCallConverters();

      assertDoesNotThrow(() -> converters.add(claiming(call -> false, null)));
    }

    /** A transform putting the given converter ahead of the built-in ones. */
    private UnaryOperator<List<CallConverter>> prepending(CallConverter converter) {
      return builtIns -> {
        List<CallConverter> converters = new ArrayList<>();
        converters.add(converter);
        converters.addAll(builtIns);
        return converters;
      };
    }

    private CallConverter claiming(Predicate<RexCall> claims, Expression result) {
      return (call, topLevelConverter) ->
          claims.test(call) ? Optional.of(result) : Optional.empty();
    }

    /** Without the extra converter, nothing in the built-in set handles the call. */
    @Test
    void areNeededForACallNoBuiltInConverterHandles() {
      assertThrows(
          IllegalArgumentException.class,
          () -> convert(ConverterProvider.builder().build(), unclaimedCall()));
    }

    @Test
    void handleACallNoBuiltInConverterHandles() {
      Expression sentinel = ExpressionCreator.i64(false, 1);
      ConverterProvider provider =
          ConverterProvider.builder()
              .callConverters(
                  prepending(claiming(call -> call.getOperator() == unclaimed, sentinel)))
              .build();

      assertEquals(sentinel, convert(provider, unclaimedCall()));
    }

    /** The built-in CAST converter is what handles this call when nothing is added. */
    @Test
    void builtInConverterHandlesCastWhenNoneAreAdded() {
      assertInstanceOf(
          Expression.Cast.class, convert(ConverterProvider.builder().build(), castCall()));
    }

    /**
     * A caller whose dialect gives a call different semantics has to be able to claim it before the
     * built-in converter for it does, which is what overriding {@link
     * ConverterProvider#getCallConverters()} allowed.
     */
    @Test
    void areConsultedBeforeTheBuiltInOnes() {
      Expression sentinel = ExpressionCreator.i64(false, 2);
      ConverterProvider provider =
          ConverterProvider.builder()
              .callConverters(
                  prepending(
                      claiming(call -> call.getOperator() == SqlStdOperatorTable.CAST, sentinel)))
              .build();

      assertEquals(sentinel, convert(provider, castCall()));
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
