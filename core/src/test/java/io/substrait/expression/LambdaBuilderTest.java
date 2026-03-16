package io.substrait.expression;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.type.TypeCreator;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for {@link LambdaBuilder} build-time validation. */
class LambdaBuilderTest {

  static final TypeCreator R = TypeCreator.REQUIRED;

  final LambdaBuilder lb = new LambdaBuilder();

  // (x: i32) -> x[5] — field index 5 is out of bounds (only 1 param)
  @Test
  void invalidFieldIndex_outOfBounds() {
    assertThrows(
        IndexOutOfBoundsException.class, () -> lb.lambda(List.of(R.I32), params -> params.ref(5)));
  }

  // (x: i32) -> x[-1] — negative field index
  @Test
  void negativeFieldIndex() {
    assertThrows(Exception.class, () -> lb.lambda(List.of(R.I32), params -> params.ref(-1)));
  }

  // (x: i32) -> (y: i64) -> x[5] — outer field index 5 is out of bounds
  @Test
  void nestedOuterFieldIndexOutOfBounds() {
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> lb.lambda(List.of(R.I32), outer -> lb.lambda(List.of(R.I64), inner -> outer.ref(5))));
  }

  // (x: i32) -> (y: i64) -> y[3] — inner field index 3 is out of bounds (only 1 param)
  @Test
  void nestedInnerFieldIndexOutOfBounds() {
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> lb.lambda(List.of(R.I32), outer -> lb.lambda(List.of(R.I64), inner -> inner.ref(3))));
  }
}
