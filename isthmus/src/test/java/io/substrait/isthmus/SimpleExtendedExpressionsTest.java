package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.isthmus.expression.RexExpressionConverter;
import java.io.IOException;
import java.util.stream.Stream;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SimpleExtendedExpressionsTest extends ExtendedExpressionTestBase {

  private static final String MARKER = "provider hook reached";

  private static Stream<Arguments> expressionTypeProvider() {
    return Stream.of(
        Arguments.of("2"), // I32LiteralExpression
        Arguments.of("L_ORDERKEY"), // FieldReferenceExpression
        Arguments.of("L_ORDERKEY > 10"), // ScalarFunctionExpressionFilter
        Arguments.of("L_ORDERKEY + 10"), // ScalarFunctionExpressionProjection
        Arguments.of("L_ORDERKEY IN (10, 20)"), // ScalarFunctionExpressionIn
        Arguments.of("L_ORDERKEY is not null"), // ScalarFunctionExpressionIsNotNull
        Arguments.of("L_ORDERKEY is null"), // ScalarFunctionExpressionIsNull
        // The rest are handled by CallConverters.defaults rather than by the scalar function
        // converter, and so reach this path only when it is assembled from the provider.
        Arguments.of("CAST(L_ORDERKEY AS VARCHAR)"), // CallConverters.CAST
        Arguments.of("CASE WHEN L_ORDERKEY > 10 THEN 1 ELSE 2 END"), // CallConverters.CASE
        Arguments.of("CURRENT_DATE"), // CallConverters.EXECUTION_CONTEXT_VARIABLE
        Arguments.of("ARRAY[1, 2]"), // SqlArrayValueConstructorCallConverter
        Arguments.of("MAP['a', 1]")); // SqlMapValueConstructorCallConverter
  }

  @ParameterizedTest
  @MethodSource("expressionTypeProvider")
  void testExtendedExpressionsRoundTrip(String sqlExpression)
      throws SqlParseException, IOException {
    assertProtoExtendedExpressionRoundtrip(sqlExpression);
  }

  @ParameterizedTest
  @MethodSource("expressionTypeProvider")
  void testExtendedExpressionsDuplicateColumnIdentifierRoundTrip(String sqlExpression) {
    IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () -> assertProtoExtendedExpressionRoundtrip(sqlExpression, "tpch/schema_error.sql"));
    assertTrue(
        illegalArgumentException
            .getMessage()
            .startsWith("There is no support for duplicate column names"));
  }

  /**
   * The converter is taken from the provider rather than assembled here, so a provider that
   * overrides {@link ConverterProvider#getRexExpressionConverter} is honoured on this path as it
   * already is when converting a plan.
   */
  @Test
  void usesTheProvidersRexExpressionConverter() throws IOException {
    ConverterProvider provider =
        new ConverterProvider(ConverterProvider.builder()) {
          @Override
          public RexExpressionConverter getRexExpressionConverter(SubstraitRelVisitor srv) {
            throw new UnsupportedOperationException(MARKER);
          }
        };

    UnsupportedOperationException e =
        assertThrows(
            UnsupportedOperationException.class, () -> new SqlExpressionToSubstrait(provider));
    assertEquals(MARKER, e.getMessage());
  }

  @Test
  void testExtendedExpressionsListExpressionRoundTrip() throws SqlParseException, IOException {
    String[] expressions = {
      "2",
      "L_ORDERKEY",
      "L_ORDERKEY > 10",
      "L_ORDERKEY + 10",
      "L_ORDERKEY IN (10, 20)",
      "L_ORDERKEY is not null",
      "L_ORDERKEY is null"
    };

    assertProtoExtendedExpressionRoundtrip(expressions);
  }
}
