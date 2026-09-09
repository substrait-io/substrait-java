package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.proto.Expression;
import io.substrait.proto.Expression.RexTypeCase;
import io.substrait.proto.ExtendedExpression;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SimpleExtendedExpressionsTest extends ExtendedExpressionTestBase {

  private static final String MARKER = "provider hook reached";

  private static final String TABLE_A = "CREATE TABLE A (A1 BIGINT, A2 BIGINT, A3 BIGINT)";
  private static final String TABLE_B = "CREATE TABLE B (B1 BIGINT, B2 BIGINT)";
  private static final String TABLE_C = "CREATE TABLE C (C1 BIGINT)";

  private static Stream<Arguments> columnSchemaProvider() {
    return Stream.of(
        Arguments.of(List.of(TABLE_A), List.of("A1", "A2", "A3")),
        Arguments.of(
            List.of(TABLE_A, TABLE_B, TABLE_C), List.of("A1", "A2", "A3", "B1", "B2", "C1")),
        Arguments.of(
            List.of(TABLE_A + ";" + TABLE_B + ";" + TABLE_C),
            List.of("A1", "A2", "A3", "B1", "B2", "C1")),
        Arguments.of(
            List.of(TABLE_B, TABLE_C, TABLE_A), List.of("B1", "B2", "C1", "A1", "A2", "A3")));
  }

  @ParameterizedTest
  @MethodSource("columnSchemaProvider")
  void fieldReferencesIndexTheCombinedSchema(List<String> tables, List<String> columnNames)
      throws SqlParseException {
    // Reverse the expression order so a reference's index cannot accidentally be its position
    // in the output expression list. All columns have the same type, so types cannot detect this.
    String[] expressions = new String[columnNames.size()];
    for (int index = 0; index < expressions.length; index++) {
      expressions[index] = columnNames.get(columnNames.size() - index - 1);
    }
    ExtendedExpression converted = new SqlExpressionToSubstrait().convert(expressions, tables);

    assertEquals(columnNames, converted.getBaseSchema().getNamesList());
    assertEquals(columnNames.size(), converted.getBaseSchema().getStruct().getTypesCount());
    assertEquals(expressions.length, converted.getReferredExprCount());
    for (int index = 0; index < expressions.length; index++) {
      assertEquals(
          columnNames.size() - index - 1,
          selectedField(converted.getReferredExpr(index).getExpression()),
          expressions[index]);
    }
  }

  @Test
  void functionArgumentsIndexTheCombinedSchema() throws SqlParseException {
    ExtendedExpression converted =
        new SqlExpressionToSubstrait()
            .convert(new String[] {"A1 = B1", "B2 + A3"}, List.of(TABLE_A, TABLE_B));

    Expression.ScalarFunction filter =
        converted.getReferredExpr(0).getExpression().getScalarFunction();
    assertEquals(0, selectedField(filter.getArguments(0).getValue()));
    assertEquals(3, selectedField(filter.getArguments(1).getValue()));
    Expression.ScalarFunction projection =
        converted.getReferredExpr(1).getExpression().getScalarFunction();
    assertEquals(4, selectedField(projection.getArguments(0).getValue()));
    assertEquals(2, selectedField(projection.getArguments(1).getValue()));
  }

  @Test
  void eachConversionBuildsItsOwnColumnIndices() throws SqlParseException {
    SqlExpressionToSubstrait converter = new SqlExpressionToSubstrait();
    ExtendedExpression multipleTables = converter.convert("B2", List.of(TABLE_A, TABLE_B));
    ExtendedExpression singleTable = converter.convert("B2", List.of(TABLE_B));

    assertEquals(4, selectedField(multipleTables.getReferredExpr(0).getExpression()));
    assertEquals(1, selectedField(singleTable.getReferredExpr(0).getExpression()));
  }

  private static int selectedField(Expression expression) {
    assertEquals(RexTypeCase.SELECTION, expression.getRexTypeCase());
    assertTrue(expression.getSelection().hasRootReference());
    assertTrue(expression.getSelection().getDirectReference().hasStructField());
    return expression.getSelection().getDirectReference().getStructField().getField();
  }

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
