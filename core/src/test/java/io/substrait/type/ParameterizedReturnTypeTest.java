package io.substrait.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.function.ParameterizedType;
import io.substrait.function.TypeExpression;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Pins the return-type derivation for the parameterized shapes, against the declarations the
 * standard extensions actually ship rather than against hand-written ones.
 */
class ParameterizedReturnTypeTest {

  private static final TypeCreator R = TypeCreator.REQUIRED;
  private static final TypeCreator N = TypeCreator.NULLABLE;
  private static final SimpleExtension.ExtensionCollection EXTENSIONS =
      DefaultExtensionCatalog.DEFAULT_COLLECTION;

  private static SimpleExtension.Function variant(String key) {
    return Stream.of(
            EXTENSIONS.scalarFunctions(),
            EXTENSIONS.aggregateFunctions(),
            EXTENSIONS.windowFunctions())
        .flatMap(List::stream)
        .filter(f -> f.key().equals(key))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no such variant: " + key));
  }

  private static Type resolve(String key, Type... args) {
    return variant(key).resolveType(List.of(args));
  }

  @Test
  void precisionTimestampCarriesItsPrecision() {
    assertEquals(
        R.precisionTimestamp(3),
        resolve("add:pts_iyear", R.precisionTimestamp(3), R.INTERVAL_YEAR));
  }

  @Test
  void intervalDayCarriesItsPrecision() {
    assertEquals(R.intervalDay(9), resolve("multiply:i8_iday", R.I8, R.intervalDay(9)));
  }

  @Test
  void dateMinusIntervalDayDerivesAPrecisionTimestamp() {
    // The signature #1117 is about: the spec declares precision_timestamp<P>, where P is the
    // interval's, and nothing but the interval carries it.
    assertEquals(R.precisionTimestamp(6), resolve("subtract:date_iday", R.DATE, R.intervalDay(6)));
    assertEquals(R.precisionTimestamp(3), resolve("subtract:date_iday", R.DATE, R.intervalDay(3)));
  }

  @Test
  void oneParameterSharedByTwoArgumentsHasToAgree() {
    assertEquals(
        R.intervalDay(3), resolve("add_intervals:iday_iday", R.intervalDay(3), R.intervalDay(3)));

    UnsupportedOperationException e =
        assertThrows(
            UnsupportedOperationException.class,
            () -> resolve("add_intervals:iday_iday", R.intervalDay(3), R.intervalDay(6)));
    assertTrue(e.getMessage().contains("P"), e.getMessage());
  }

  @Test
  void varCharAndFixedCharCarryTheirLength() {
    assertEquals(R.varChar(20), resolve("concat:vchar", R.varChar(20), R.varChar(20)));
    assertEquals(R.fixedChar(8), resolve("reverse:fchar", R.fixedChar(8)));
  }

  @Test
  void theOtherTemporalShapesCarryTheirPrecision() {
    assertEquals(N.precisionTime(3), resolve("min:pt", R.precisionTime(3)));
    assertEquals(
        R.precisionTimestampTZ(9),
        resolve("add:ptstz_iyear_str", R.precisionTimestampTZ(9), R.INTERVAL_YEAR, R.STRING));
  }

  /**
   * fixedbinary and interval_compound are the two parameterized shapes no standard extension
   * declares as a return, so they are pinned against a hand-written declaration instead of the
   * catalog.
   */
  @Test
  void theShapesTheCatalogDoesNotDeclareDeriveToo() {
    assertEquals(
        R.fixedBinary(9),
        derive(
            ParameterizedType.FixedBinary.builder().nullable(false).length(parameter("L1")).build(),
            R.fixedBinary(9)));
    assertEquals(
        R.intervalCompound(3),
        derive(
            ParameterizedType.IntervalCompound.builder()
                .nullable(false)
                .precision(parameter("P"))
                .build(),
            R.intervalCompound(3)));
  }

  private static ParameterizedType.StringLiteral parameter(String name) {
    return ParameterizedType.StringLiteral.builder().nullable(false).value(name).build();
  }

  /** Derives the return of a one-argument declaration whose argument has the return's own shape. */
  private static Type derive(ParameterizedType declaredReturn, Type actual) {
    return TypeExpressionEvaluator.evaluateExpression(
        declaredReturn,
        List.of(SimpleExtension.ValueArgument.builder().value(declaredReturn).name("arg1").build()),
        List.of(actual));
  }

  @Test
  void mirrorNullabilityStillApplies() {
    // The declared return is non-null; MIRROR makes it nullable because an argument is.
    assertEquals(N.intervalDay(6), resolve("multiply:i8_iday", R.I8, N.intervalDay(6)));
  }

  /**
   * The two return shapes the evaluator does not derive, pinned by the variants that carry them so
   * that the list in {@link TypeExpressionEvaluator}'s Javadoc cannot go stale on its own. A
   * parameter that is a type to evaluate rather than an integer to substitute is the first; a
   * multi-line return program is the second.
   */
  @Test
  void theReturnShapesThatAreNotDerivedYet() {
    assertEquals(
        List.of(
            "filter:list_func",
            "quantile:req_req_i64_any",
            "regexp_match_substring_all:vchar_vchar_i64_i64",
            "regexp_string_split:vchar_vchar",
            "sort:list",
            "string_split:vchar_vchar",
            "transform:list_func"),
        variantsReturning(ParameterizedType.ListType.class));

    assertEquals(
        List.of(
            "add:dec_dec",
            "assume_timezone:date_str_i8",
            "bitwise_and:dec_dec",
            "bitwise_or:dec_dec",
            "bitwise_xor:dec_dec",
            "ceil:dec",
            "divide:dec_dec",
            "floor:dec",
            "modulus:dec_dec",
            "multiply:dec_dec",
            "round:dec_i32",
            "strptime_time:str_str_i8",
            "strptime_timestamp:str_str_i8",
            "strptime_timestamp:str_str_str_i8",
            "subtract:dec_dec"),
        variantsReturning(TypeExpression.ReturnProgram.class));
  }

  private static List<String> variantsReturning(Class<?> returnShape) {
    return Stream.of(
            EXTENSIONS.scalarFunctions(),
            EXTENSIONS.aggregateFunctions(),
            EXTENSIONS.windowFunctions())
        .flatMap(List::stream)
        .filter(f -> returnShape.isInstance(f.returnType()))
        .map(SimpleExtension.Function::key)
        .distinct()
        .sorted()
        .collect(Collectors.toList());
  }
}
