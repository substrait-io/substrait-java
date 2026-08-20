package io.substrait.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import java.util.List;
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

  @Test
  void mirrorNullabilityStillApplies() {
    // The declared return is non-null; MIRROR makes it nullable because an argument is.
    assertEquals(N.intervalDay(6), resolve("multiply:i8_iday", R.I8, N.intervalDay(6)));
  }
}
