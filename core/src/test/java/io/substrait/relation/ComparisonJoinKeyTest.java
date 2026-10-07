package io.substrait.relation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.TestBase;
import io.substrait.expression.FieldReference;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.relation.physical.ComparisonJoinKey;
import io.substrait.relation.physical.ComparisonJoinKey.CustomComparison;
import io.substrait.type.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ComparisonJoinKeyTest extends TestBase {

  @Test
  void rejectsNonBinaryComparator() {
    SimpleExtension.ScalarFunctionVariant not =
        scalar(DefaultExtensionCatalog.FUNCTIONS_BOOLEAN, "not:bool");
    assertThrows(IllegalArgumentException.class, () -> CustomComparison.of(not));
  }

  @Test
  void rejectsNonBooleanComparator() {
    SimpleExtension.ScalarFunctionVariant add =
        scalar(DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "add:i32_i32");
    assertThrows(IllegalArgumentException.class, () -> CustomComparison.of(add));
  }

  @ParameterizedTest
  @ValueSource(strings = {"nullif:any_any", "coalesce:any"})
  void acceptsBooleanReturnResolvedFromKeyTypes(String key) {
    CustomComparison comparison =
        CustomComparison.of(scalar(DefaultExtensionCatalog.FUNCTIONS_COMPARISON, key));
    assertDoesNotThrow(() -> joinKey(comparison, N.BOOLEAN));
  }

  @ParameterizedTest
  @ValueSource(strings = {"nullif:any_any", "coalesce:any"})
  void rejectsNonBooleanReturnResolvedFromKeyTypes(String key) {
    CustomComparison comparison =
        CustomComparison.of(scalar(DefaultExtensionCatalog.FUNCTIONS_COMPARISON, key));
    assertThrows(IllegalArgumentException.class, () -> joinKey(comparison, R.I32));
  }

  @Test
  void rejectsKeyTypesThatCannotBindToComparatorParameters() {
    CustomComparison decimalAdd =
        CustomComparison.of(
            scalar(DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC_DECIMAL, "add:dec_dec"));
    assertThrows(IllegalArgumentException.class, () -> joinKey(decimalAdd, R.I32));

    CustomComparison nullIf =
        CustomComparison.of(scalar(DefaultExtensionCatalog.FUNCTIONS_COMPARISON, "nullif:any_any"));
    assertThrows(IllegalArgumentException.class, () -> joinKey(nullIf, N.BOOLEAN, R.I32));
  }

  private SimpleExtension.ScalarFunctionVariant scalar(String urn, String key) {
    return extensions.getScalarFunction(SimpleExtension.FunctionAnchor.of(urn, key));
  }

  private ComparisonJoinKey joinKey(CustomComparison comparison, Type type) {
    return joinKey(comparison, type, type);
  }

  private ComparisonJoinKey joinKey(CustomComparison comparison, Type leftType, Type rightType) {
    return ComparisonJoinKey.builder()
        .left(FieldReference.newRootStructReference(0, leftType))
        .right(FieldReference.newRootStructReference(0, rightType))
        .comparison(comparison)
        .build();
  }
}
