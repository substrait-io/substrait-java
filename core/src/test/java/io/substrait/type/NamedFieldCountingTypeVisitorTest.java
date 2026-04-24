package io.substrait.type;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class NamedFieldCountingTypeVisitorTest {
  private static final TypeCreator R = TypeCreator.REQUIRED;

  @Test
  void primitiveTypeHasNoNames() {
    assertEquals(0, NamedFieldCountingTypeVisitor.countNames(R.I32));
  }

  @Test
  void simpleNamedStructDocExample() {
    // Doc example from https://substrait.io/types/named_structs/ "Simple Named Struct".
    Type.Struct type = R.struct(R.I64, R.list(R.I64), R.map(R.I64, R.I64), R.I64);

    assertEquals(4, NamedFieldCountingTypeVisitor.countNames(type));
  }

  @Test
  void structsInCompoundTypesDocExample() {
    // Doc example from https://substrait.io/types/named_structs/ "Structs in Compound Types".
    Type.Struct type =
        R.struct(
            R.I64, R.list(R.struct(R.I64, R.I64)), R.map(R.I64, R.struct(R.I64, R.I64)), R.I64);

    assertEquals(8, NamedFieldCountingTypeVisitor.countNames(type));
  }

  @Test
  void structsInStructsDocExample() {
    // Doc example from https://substrait.io/types/named_structs/ "Structs in Structs".
    Type.Struct type =
        R.struct(R.I64, R.struct(R.I64, R.struct(R.I64, R.I64), R.I64, R.struct(R.I64, R.I64)));

    assertEquals(10, NamedFieldCountingTypeVisitor.countNames(type));
  }
}
