package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.stream.Stream;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SimpleExtendedExpressionsTest extends ExtendedExpressionTestBase {

  private static Stream<Arguments> expressionTypeProvider() {
    return Stream.of(
        Arguments.of("2"), // I32LiteralExpression
        Arguments.of("L_ORDERKEY"), // FieldReferenceExpression
        Arguments.of("L_ORDERKEY > 10"), // ScalarFunctionExpressionFilter
        Arguments.of("L_ORDERKEY + 10"), // ScalarFunctionExpressionProjection
        Arguments.of("L_ORDERKEY IN (10, 20)"), // ScalarFunctionExpressionIn
        Arguments.of("L_ORDERKEY is not null"), // ScalarFunctionExpressionIsNotNull
        Arguments.of("L_ORDERKEY is null")); // ScalarFunctionExpressionIsNull
  }

  @ParameterizedTest
  @MethodSource("expressionTypeProvider")
  void testExtendedExpressionsRoundTrip(final String sqlExpression)
      throws SqlParseException, IOException {
    assertProtoExtendedExpressionRoundtrip(sqlExpression);
  }

  @ParameterizedTest
  @MethodSource("expressionTypeProvider")
  void testExtendedExpressionsDuplicateColumnIdentifierRoundTrip(final String sqlExpression) {
    final IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () -> assertProtoExtendedExpressionRoundtrip(sqlExpression, "tpch/schema_error.sql"));
    assertTrue(
        illegalArgumentException
            .getMessage()
            .startsWith("There is no support for duplicate column names"));
  }

  @Test
  void testExtendedExpressionsListExpressionRoundTrip() throws SqlParseException, IOException {
    final String[] expressions = {
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
