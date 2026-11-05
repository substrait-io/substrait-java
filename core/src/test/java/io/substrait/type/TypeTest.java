package io.substrait.type;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Tests for the default methods on {@link Type}. */
class TypeTest {
  private static final TypeCreator R = TypeCreator.REQUIRED;
  private static final TypeCreator N = TypeCreator.NULLABLE;

  @Test
  void integerWidthsAreIntegers() {
    assertTrue(R.I8.isInteger());
    assertTrue(R.I16.isInteger());
    assertTrue(R.I32.isInteger());
    assertTrue(R.I64.isInteger());
  }

  @Test
  void nullabilityIsIrrelevantForIsInteger() {
    assertTrue(N.I8.isInteger());
    assertTrue(N.I16.isInteger());
    assertTrue(N.I32.isInteger());
    assertTrue(N.I64.isInteger());
  }

  @Test
  void nonIntegerPrimitivesAreNotIntegers() {
    assertFalse(R.BOOLEAN.isInteger());
    assertFalse(R.FP32.isInteger());
    assertFalse(R.FP64.isInteger());
    assertFalse(R.STRING.isInteger());
    assertFalse(R.BINARY.isInteger());
    assertFalse(R.DATE.isInteger());
    assertFalse(R.UUID.isInteger());
  }

  @Test
  void compoundAndSpecialTypesAreNotIntegers() {
    assertFalse(R.decimal(10, 2).isInteger());
    assertFalse(R.struct(R.I64).isInteger());
    assertFalse(R.list(R.I64).isInteger());
    assertFalse(R.map(R.I64, R.I64).isInteger());
  }

  @Test
  void userDefinedIsNotAnInteger() {
    assertFalse(R.userDefined("urn:test", "t").isInteger());
  }
}
