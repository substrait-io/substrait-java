package io.substrait.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.function.ParameterizedType;
import io.substrait.function.ParameterizedTypeCreator;
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
    // subtract(date, interval_day<P>) declares precision_timestamp<P>, and nothing but the
    // interval carries P.
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
    assertTrue(
        e.getMessage().contains("Inconsistent binding for type parameter 'P'"), e.getMessage());
  }

  @Test
  void aParameterizedDeclarationRejectsAnotherActualShape() {
    UnsupportedOperationException e =
        assertThrows(
            UnsupportedOperationException.class,
            () -> resolve("concat:vchar", R.varChar(20), R.STRING));

    assertTrue(
        e.getMessage().contains("Cannot bind parameters from declared argument type"),
        e.getMessage());
  }

  @Test
  void aContainerDeclarationIsNotRefusedForABindingItNeverMakes() {
    // Binding descends into none of the container declarations, so a `list<any1>` or a
    // `func<any1 -> boolean?>` argument binds nothing. All four of these declare a concrete return
    // and need no binding at all, so refusing the shape would reject calls that resolve today.
    assertEquals(R.I64, resolve("cardinality:list", R.list(R.I64)));
    assertEquals(N.I64, resolve("index_in:any_list", R.I64, R.list(R.I64)));
    assertEquals(N.BOOLEAN, resolve("all_match:list_func", R.list(R.I64), N.BOOLEAN));
    assertEquals(N.BOOLEAN, resolve("any_match:list_func", R.list(R.I64), N.BOOLEAN));
  }

  @Test
  void aConcreteReturnStillChecksSharedParameters() {
    UnsupportedOperationException comparison =
        assertThrows(
            UnsupportedOperationException.class,
            () -> resolve("lt:any_any", R.precisionTimestamp(3), R.precisionTimestamp(6)));
    assertTrue(
        comparison.getMessage().contains("Inconsistent binding for type parameter 'any1'"),
        comparison.getMessage());

    UnsupportedOperationException strpos =
        assertThrows(
            UnsupportedOperationException.class,
            () -> resolve("strpos:vchar_vchar", R.varChar(20), R.varChar(3)));
    assertTrue(
        strpos.getMessage().contains("Inconsistent binding for type parameter 'L1'"),
        strpos.getMessage());
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
   * declares at all, as an argument or as a return, so they are pinned against a hand-written
   * declaration instead of the catalog.
   */
  @Test
  void theShapesTheCatalogDoesNotDeclareDeriveToo() {
    ParameterizedTypeCreator P = ParameterizedTypeCreator.REQUIRED;
    assertEquals(R.fixedBinary(9), derive(P.fixedBinaryE("L1"), R.fixedBinary(9)));
    assertEquals(R.intervalCompound(3), derive(P.intervalCompoundE("P"), R.intervalCompound(3)));
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
   * The census of list returns the evaluator does not derive. The catalog is owned upstream, so
   * this catches declarations added by a {@code substrait-packaging} bump.
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

    assertThrows(
        UnsupportedOperationException.class,
        () -> resolve("string_split:vchar_vchar", R.varChar(20), R.varChar(20)));
  }

  @Test
  void catalogReturnProgramsAreCovered() {
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

    // Arithmetic programs are exercised with exact expectations in ReturnProgramTypeTest.
    // The rounding programs derive from the input's precision and scale too, including round:
    // the pinned declaration does not read its value argument s.
    assertEquals(R.decimal(9, 0), resolve("ceil:dec", R.decimal(10, 2)));
    assertEquals(R.decimal(9, 0), resolve("floor:dec", R.decimal(10, 2)));
    assertEquals(N.decimal(11, 2), resolve("round:dec_i32", R.decimal(10, 2), R.I32));

    // These four declarations read integer_parameter(precision) without binding precision from
    // any argument type. Supplying an i8 type cannot provide that argument's value.
    for (String key : List.of("strptime_time:str_str_i8", "strptime_timestamp:str_str_i8")) {
      assertUnboundPrecision(key, R.STRING, R.STRING, R.I8);
    }
    assertUnboundPrecision("strptime_timestamp:str_str_str_i8", R.STRING, R.STRING, R.STRING, R.I8);
    assertUnboundPrecision("assume_timezone:date_str_i8", R.DATE, R.STRING, R.I8);
  }

  private static void assertUnboundPrecision(String key, Type... arguments) {
    UnsupportedOperationException error =
        assertThrows(UnsupportedOperationException.class, () -> resolve(key, arguments));
    assertTrue(
        error.getMessage().contains("Unbound type parameter 'precision'"), error.getMessage());
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
