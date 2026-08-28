package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.proto.Expression.RexTypeCase;
import io.substrait.proto.ExtendedExpression;
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
        Arguments.of("L_ORDERKEY is null")); // ScalarFunctionExpressionIsNull
  }

  /**
   * The expressions the scalar function converter does not claim, which reach this path only once
   * the converter is assembled from the provider. Each is paired with what it converts into: the
   * round trip these share with the cases above compares a proto against itself, so it holds
   * whatever the conversion produced.
   */
  private static Stream<Arguments> callConverterExpressionProvider() {
    return Stream.of(
        Arguments.of("CAST(L_ORDERKEY AS VARCHAR)", RexTypeCase.CAST),
        Arguments.of("CASE WHEN L_ORDERKEY > 10 THEN 1 ELSE 2 END", RexTypeCase.IF_THEN),
        Arguments.of("CURRENT_DATE", RexTypeCase.EXECUTION_CONTEXT_VARIABLE),
        Arguments.of("ARRAY[1, 2]", RexTypeCase.LITERAL),
        Arguments.of("MAP['a', 1]", RexTypeCase.LITERAL),
        Arguments.of("ROW_NUMBER() OVER (ORDER BY L_ORDERKEY)", RexTypeCase.WINDOW_FUNCTION));
  }

  @ParameterizedTest
  @MethodSource("callConverterExpressionProvider")
  void aCallTheScalarConverterDoesNotClaimIsConverted(String sqlExpression, RexTypeCase expected)
      throws SqlParseException, IOException {
    ExtendedExpression extendedExpression =
        new SqlExpressionToSubstrait(ConverterProvider.DEFAULT)
            .convert(sqlExpression, tpchSchemaCreateStatements());

    assertEquals(expected, extendedExpression.getReferredExpr(0).getExpression().getRexTypeCase());
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
  void usesTheProvidersRexExpressionConverter() {
    SubstraitRelVisitor[] seen = new SubstraitRelVisitor[1];
    ConverterProvider provider =
        new ConverterProvider(ConverterProvider.builder()) {
          @Override
          public RexExpressionConverter getRexExpressionConverter(SubstraitRelVisitor srv) {
            seen[0] = srv;
            throw new UnsupportedOperationException(MARKER);
          }
        };

    UnsupportedOperationException e =
        assertThrows(
            UnsupportedOperationException.class, () -> new SqlExpressionToSubstrait(provider));
    assertEquals(MARKER, e.getMessage());
    assertNull(seen[0]);
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
