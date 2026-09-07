package io.substrait.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.function.ParameterizedTypeCreator;
import io.substrait.type.parser.TypeStringParser;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ReturnProgramTypeTest {

  private static final TypeCreator R = TypeCreator.REQUIRED;
  private static final TypeCreator N = TypeCreator.NULLABLE;
  private static final String URN = DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC_DECIMAL;

  private static Type resolve(String key, Type... arguments) {
    return DefaultExtensionCatalog.DEFAULT_COLLECTION
        .getScalarFunction(SimpleExtension.FunctionAnchor.of(URN, key))
        .resolveType(List.of(arguments));
  }

  @ParameterizedTest
  @CsvSource({
    "add,10,2,5,1,11,2",
    "add,38,10,38,10,38,9",
    "subtract,10,2,5,1,11,2",
    "subtract,38,10,38,10,38,9",
    "multiply,10,2,5,1,16,3",
    "multiply,38,10,38,10,38,6",
    "multiply,30,20,30,20,38,17",
    "divide,10,2,5,1,21,8",
    "divide,38,10,38,10,38,6",
    "modulus,10,2,5,1,6,2"
  })
  void decimalProgramsDeriveTheCatalogFormula(
      String name, int p1, int s1, int p2, int s2, int precision, int scale) {
    // Expected types follow the extension's spec v0.102.0 formulas. In particular, divide's
    // precision uses P2, where the separate prose example uses S2.
    assertEquals(
        R.decimal(precision, scale),
        resolve(name + ":dec_dec", R.decimal(p1, s1), R.decimal(p2, s2)));
  }

  @Test
  void decimalProgramsPreserveMirrorNullabilityAndLiteralConstraints() {
    assertEquals(N.decimal(11, 2), resolve("add:dec_dec", N.decimal(10, 2), R.decimal(5, 1)));
    for (String name : List.of("bitwise_and", "bitwise_or", "bitwise_xor")) {
      assertEquals(
          R.decimal(20, 0), resolve(name + ":dec_dec", R.decimal(10, 0), R.decimal(20, 0)));
      assertThrows(
          UnsupportedOperationException.class,
          () -> resolve(name + ":dec_dec", R.decimal(10, 1), R.decimal(20, 0)));
    }
  }

  private static Type evaluate(String expression) {
    return TypeExpressionEvaluator.evaluateExpression(
        TypeStringParser.parseExpression(expression, URN),
        List.of(
            SimpleExtension.ValueArgument.builder()
                .name("input")
                .value(ParameterizedTypeCreator.REQUIRED.varCharE("L"))
                .build()),
        List.of(R.varChar(10)));
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = ';',
      value = {
        "varchar<L + 1>; varchar<11>",
        "fixedchar<L * 2>; fixedchar<20>",
        "fixedbinary<L / 2>; fixedbinary<5>",
        "decimal<L + 2, L - 8>; decimal<12,2>",
        "precision_time<min(L, 6)>; precision_time<6>",
        "precision_timestamp<max(3, L - 4)>; precision_timestamp<6>",
        "precision_timestamp_tz<L - 4>; precision_timestamp_tz<6>",
        "interval_day<L - 7>; interval_day<3>",
        "interval_compound<L - 7>; interval_compound<3>"
      })
  void arithmeticWorksInsideTypeParameters(String expression, String expected) {
    assertEquals(TypeStringParser.parseSimple(expected, URN), evaluate(expression));
  }

  @Test
  void assignmentsUseEarlierResultsAndRemainLocalToOneEvaluation() {
    assertEquals(R.varChar(24), evaluate("a = L + 2\nb = a * 2\nvarchar<b>"));
    assertEquals(R.varChar(10), evaluate("wide = L > 5\nvarchar<wide ? L : 1>"));
    assertEquals(R.varChar(22), evaluate("L = L + 1\nL = L * 2\nvarchar<L>"));
    assertEquals(R.varChar(10), evaluate("varchar<L>"));
    assertThrows(UnsupportedOperationException.class, () -> evaluate("varchar<a>"));
  }

  @Test
  void conditionsSelectOnlyTheChosenBranch() {
    assertEquals(R.varChar(10), evaluate("varchar<L > 5 ? L : missing>"));
    assertEquals(R.varChar(10), evaluate("varchar<L < 5 ? 1 / 0 : L>"));
    assertEquals(R.I64, evaluate("(L = 10) ? i64 : string"));
    assertEquals(R.varChar(10), evaluate("varchar<!(L < 5) AND L > 0 ? L : 1>"));
    assertEquals(R.varChar(10), evaluate("varchar<L = 10 OR L < 0 ? L : 1>"));
    assertEquals(N.I64, evaluate("if L >= 10 then i64? else string"));
    assertEquals(R.STRING, evaluate("L != 10 ? i64 : string"));
    assertEquals(R.varChar(2), evaluate("varchar<(L <= 10 AND L >= 10 AND L != 9) ? 2 : 1>"));
    assertEquals(R.varChar(1), evaluate("varchar<(L <= 9 OR L >= 11 OR L != 10) ? 2 : 1>"));
  }

  @Test
  void booleanOperationsEvaluateBothOperands() {
    assertArithmeticFailure("varchar<(L < 0 AND L / 0 > 0) ? 1 : 2>");
    assertArithmeticFailure("varchar<(L > 0 OR L / 0 > 0) ? 1 : 2>");
  }

  @Test
  void signedDivisionTruncatesTowardsZero() {
    assertEquals(R.varChar(13), evaluate("varchar<L - (0 - L) / 3>"));
    assertEquals(R.varChar(7), evaluate("varchar<L + L / (0 - 3)>"));
  }

  @Test
  void integerExpressionsUse64BitsBeforeConvertingToATypeParameter() {
    assertEquals(R.varChar(10), evaluate("wide = 2147483647 + L\nvarchar<wide - 2147483647>"));
    assertArithmeticFailure("varchar<2147483647 + L>");
    assertArithmeticFailure("wide = 2147483647 * 2147483647 * L\nvarchar<10>");

    String minimum = "low = (0 - 2147483647 - 1) * (2147483647 + 1) * 2\n";
    assertEquals(R.varChar(10), evaluate(minimum + "varchar<low / low * L>"));
    for (String expression : List.of("low - 1", "(0 - (low + 1)) + 1", "low / (0 - 1)")) {
      // Overflow must fail even when the assignment's result is not used by the final type.
      assertArithmeticFailure(minimum + "wide = " + expression + "\nvarchar<10>");
    }
  }

  @Test
  void invalidExpressionKindsAndUnboundParametersAreRejected() {
    for (String expression : List.of("varchar<L > 0>", "varchar<L ? 1 : 2>")) {
      assertThrows(UnsupportedOperationException.class, () -> evaluate(expression), expression);
    }
    assertArithmeticFailure("varchar<L / 0>");
    UnsupportedOperationException error =
        assertThrows(UnsupportedOperationException.class, () -> evaluate("varchar<missing + 1>"));
    assertTrue(error.getMessage().contains("missing"), error.getMessage());
  }

  private static void assertArithmeticFailure(String expression) {
    UnsupportedOperationException error =
        assertThrows(UnsupportedOperationException.class, () -> evaluate(expression), expression);
    assertInstanceOf(ArithmeticException.class, error.getCause(), expression);
  }
}
