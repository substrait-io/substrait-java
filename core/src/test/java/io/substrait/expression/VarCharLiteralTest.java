package io.substrait.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.TestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class VarCharLiteralTest extends TestBase {

  @ParameterizedTest
  @CsvSource({"abc, 3", "abc, 5", "'a😀', 2", "'é', 2", "'', 0"})
  void fittingValuesRoundTrip(String value, int length) {
    for (boolean nullable : new boolean[] {false, true}) {
      Expression.VarCharLiteral literal = ExpressionCreator.varChar(nullable, value, length);
      assertEquals(value, literal.value());
      assertEquals(length, literal.length());
      assertEquals(nullable, literal.nullable());
      verifyRoundTrip(literal);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void builderRejectsOverlongValue(boolean nullable) {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            Expression.VarCharLiteral.builder()
                .nullable(nullable)
                .value("abcdef")
                .length(3)
                .build());
  }

  @ParameterizedTest
  @CsvSource({"abcdef, 3", "'a😀', 1", "'é', 1"})
  void factoryRejectsOverlongValue(String value, int length) {
    assertThrows(
        IllegalArgumentException.class, () -> ExpressionCreator.varChar(false, value, length));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void protoRejectsOverlongValue(boolean nullable) {
    io.substrait.proto.Expression proto =
        io.substrait.proto.Expression.newBuilder()
            .setLiteral(
                io.substrait.proto.Expression.Literal.newBuilder()
                    .setNullable(nullable)
                    .setVarChar(
                        io.substrait.proto.Expression.Literal.VarChar.newBuilder()
                            .setValue("abcdef")
                            .setLength(3)))
            .build();
    assertThrows(IllegalArgumentException.class, () -> protoExpressionConverter.from(proto));
  }
}
