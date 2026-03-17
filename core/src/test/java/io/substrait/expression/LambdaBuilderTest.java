package io.substrait.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for {@link LambdaBuilder}. */
class LambdaBuilderTest {

  static final TypeCreator R = TypeCreator.REQUIRED;

  final LambdaBuilder lb = new LambdaBuilder();

  // (x: i32)@p -> p[0]
  @Test
  void simpleLambda() {
    Expression.Lambda lambda = lb.lambda(List.of(R.I32), params -> params.ref(0));

    Expression.Lambda expected =
        ImmutableExpression.Lambda.builder()
            .parameters(Type.Struct.builder().nullable(false).addFields(R.I32).build())
            .body(
                FieldReference.newLambdaParameterReference(
                    0, Type.Struct.builder().nullable(false).addFields(R.I32).build(), 0))
            .build();

    assertEquals(expected, lambda);
  }

  // (x: i32)@outer -> (y: i64)@inner -> outer[0]
  @Test
  void nestedLambda() {
    Expression.Lambda lambda =
        lb.lambda(List.of(R.I32), outer -> lb.lambda(List.of(R.I64), inner -> outer.ref(0)));

    Expression.Lambda expectedInner =
        ImmutableExpression.Lambda.builder()
            .parameters(Type.Struct.builder().nullable(false).addFields(R.I64).build())
            .body(
                FieldReference.newLambdaParameterReference(
                    0, Type.Struct.builder().nullable(false).addFields(R.I32).build(), 1))
            .build();

    Expression.Lambda expected =
        ImmutableExpression.Lambda.builder()
            .parameters(Type.Struct.builder().nullable(false).addFields(R.I32).build())
            .body(expectedInner)
            .build();

    assertEquals(expected, lambda);
  }

  // Verify that the same scope handle produces different stepsOut values depending on nesting.
  // outer.ref(0) should produce stepsOut=0 at the top level and stepsOut=1 inside a nested lambda.
  @Test
  void scopeStepsOutChangesDynamically() {
    lb.lambda(
        List.of(R.I32),
        outer -> {
          FieldReference atTopLevel = outer.ref(0);
          assertEquals(0, atTopLevel.lambdaParameterReferenceStepsOut().orElse(-1));

          lb.lambda(
              List.of(R.I64),
              inner -> {
                FieldReference atNestedLevel = outer.ref(0);
                assertEquals(1, atNestedLevel.lambdaParameterReferenceStepsOut().orElse(-1));
                return inner.ref(0);
              });

          return atTopLevel;
        });
  }

  // (x: i32)@p -> p[5] — only 1 param, index 5 is out of bounds
  @Test
  void invalidFieldIndex_outOfBounds() {
    assertThrows(
        IndexOutOfBoundsException.class, () -> lb.lambda(List.of(R.I32), params -> params.ref(5)));
  }

  // (x: i32)@p -> p[-1] — negative index
  @Test
  void negativeFieldIndex() {
    assertThrows(Exception.class, () -> lb.lambda(List.of(R.I32), params -> params.ref(-1)));
  }

  // (x: i32)@outer -> (y: i64)@inner -> outer[5] — outer only has 1 param
  @Test
  void nestedOuterFieldIndexOutOfBounds() {
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> lb.lambda(List.of(R.I32), outer -> lb.lambda(List.of(R.I64), inner -> outer.ref(5))));
  }

  // (x: i32)@outer -> (y: i64)@inner -> inner[3] — inner only has 1 param
  @Test
  void nestedInnerFieldIndexOutOfBounds() {
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> lb.lambda(List.of(R.I32), outer -> lb.lambda(List.of(R.I64), inner -> inner.ref(3))));
  }
}
