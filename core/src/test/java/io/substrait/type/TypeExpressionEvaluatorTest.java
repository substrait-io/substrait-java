package io.substrait.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import java.util.List;
import org.junit.jupiter.api.Test;

class TypeExpressionEvaluatorTest {

  final SimpleExtension.ExtensionCollection extensions = DefaultExtensionCatalog.DEFAULT_COLLECTION;

  private SimpleExtension.AggregateFunctionVariant aggregate(String urn, String key) {
    return extensions.getAggregateFunction(SimpleExtension.FunctionAnchor.of(urn, key));
  }

  private SimpleExtension.ScalarFunctionVariant scalar(String urn, String key) {
    return extensions.getScalarFunction(SimpleExtension.FunctionAnchor.of(urn, key));
  }

  @Test
  void concreteIntegerSumPassesThrough() {
    SimpleExtension.AggregateFunctionVariant sum =
        aggregate(DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "sum:i32");
    // sum:i32 declares a concrete "i64?" return, independent of the argument.
    assertEquals(TypeCreator.NULLABLE.I64, sum.resolveType(List.of(TypeCreator.REQUIRED.I32)));
  }

  @Test
  void decimalSumDerivesWidthAndScale() {
    SimpleExtension.AggregateFunctionVariant sum =
        aggregate(DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC_DECIMAL, "sum:dec");
    // sum:dec declares DECIMAL<P,S> -> DECIMAL?<38,S>: precision widens to 38, scale mirrors S.
    assertEquals(
        TypeCreator.NULLABLE.decimal(38, 2),
        sum.resolveType(List.of(TypeCreator.REQUIRED.decimal(10, 2))));
    assertEquals(
        TypeCreator.NULLABLE.decimal(38, 7),
        sum.resolveType(List.of(TypeCreator.REQUIRED.decimal(20, 7))));
  }

  @Test
  void rejectsInconsistentWildcardBinding() {
    SimpleExtension.ScalarFunctionVariant nullif =
        scalar(DefaultExtensionCatalog.FUNCTIONS_COMPARISON, "nullif:any_any");
    // nullif(any1, any1) -> any1?: binding the same numbered wildcard to two different types is a
    // signature error, and must be rejected rather than silently resolved to the first binding.
    assertThrows(
        UnsupportedOperationException.class,
        () -> nullif.resolveType(List.of(TypeCreator.REQUIRED.I32, TypeCreator.REQUIRED.STRING)));
  }

  @Test
  void variadicWildcardBindsEveryArgument() {
    SimpleExtension.ScalarFunctionVariant coalesce =
        scalar(DefaultExtensionCatalog.FUNCTIONS_COMPARISON, "coalesce:any");
    // coalesce(any1...) states its argument once and accepts it repeatedly, so an argument past
    // the declared tail still has to agree with the wildcard.
    assertThrows(
        UnsupportedOperationException.class,
        () -> coalesce.resolveType(List.of(TypeCreator.REQUIRED.I32, TypeCreator.REQUIRED.STRING)));
    assertEquals(
        TypeCreator.REQUIRED.I32,
        coalesce.resolveType(
            List.of(TypeCreator.REQUIRED.I32, TypeCreator.REQUIRED.I32, TypeCreator.REQUIRED.I32)));
  }

  @Test
  void appliesMirrorNullabilityPolicy() {
    // add:i32_i32 carries the (default) MIRROR policy: its concrete "i32" return mirrors the value
    // arguments' nullability, so add(i32?, i32) is i32? — per the spec's own worked example.
    SimpleExtension.ScalarFunctionVariant add =
        scalar(DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "add:i32_i32");
    assertEquals(
        TypeCreator.REQUIRED.I32,
        add.resolveType(List.of(TypeCreator.REQUIRED.I32, TypeCreator.REQUIRED.I32)));
    assertEquals(
        TypeCreator.NULLABLE.I32,
        add.resolveType(List.of(TypeCreator.NULLABLE.I32, TypeCreator.REQUIRED.I32)));
  }

  @Test
  void wildcardBindingIgnoresNullability() {
    SimpleExtension.ScalarFunctionVariant nullif =
        scalar(DefaultExtensionCatalog.FUNCTIONS_COMPARISON, "nullif:any_any");
    // Nullability is not part of a wildcard's identity: i32 and i32? bind the same any1. The
    // declared "any1?" return then decides the result's nullability.
    assertEquals(
        TypeCreator.NULLABLE.I32,
        nullif.resolveType(List.of(TypeCreator.REQUIRED.I32, TypeCreator.NULLABLE.I32)));
  }
}
