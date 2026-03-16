package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.expression.LambdaBuilder;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.type.Type;
import java.util.List;
import org.junit.jupiter.api.Test;

class LambdaExpressionRoundtripTest extends TestBase {

  final LambdaBuilder lb = new LambdaBuilder();

  // ==================== Single Lambda Tests ====================

  // () -> 42
  @Test
  void zeroParameterLambda() {
    Expression.Lambda lambda = lb.lambda(List.of(), params -> ExpressionCreator.i32(false, 42));

    verifyRoundTrip(lambda);

    Type.Func funcType = (Type.Func) lambda.getType();
    assertEquals(0, funcType.parameterTypes().size());
    assertEquals(R.I32, funcType.returnType());
  }

  // (x: i32) -> x
  @Test
  void identityLambda() {
    Expression.Lambda lambda = lb.lambda(List.of(R.I32), params -> params.ref(0));

    verifyRoundTrip(lambda);

    Type.Func funcType = (Type.Func) lambda.getType();
    assertEquals(1, funcType.parameterTypes().size());
    assertEquals(R.I32, funcType.parameterTypes().get(0));
    assertEquals(R.I32, funcType.returnType());

    assertInstanceOf(FieldReference.class, lambda.body());
    FieldReference ref = (FieldReference) lambda.body();
    assertTrue(ref.isLambdaParameterReference());
    assertEquals(0, ref.lambdaParameterReferenceStepsOut().orElse(-1));
  }

  // (x: i32, y: i64, z: string) -> z
  @Test
  void validFieldIndex() {
    Expression.Lambda lambda = lb.lambda(List.of(R.I32, R.I64, R.STRING), params -> params.ref(2));

    verifyRoundTrip(lambda);
    assertEquals(R.STRING, ((Type.Func) lambda.getType()).returnType());
  }

  // (x: i32) -> 42
  @Test
  void lambdaWithLiteralBody() {
    Expression.Lambda lambda =
        lb.lambda(List.of(R.I32), params -> ExpressionCreator.i32(false, 42));

    verifyRoundTrip(lambda);
    assertInstanceOf(Expression.I32Literal.class, lambda.body());
  }

  // Parameterized: (params...) -> params[fieldIndex], verifying type resolution
  @Test
  void typeResolution() {
    record TestCase(String name, List<Type> paramTypes, int fieldIndex, Type expectedType) {}

    List<TestCase> testCases =
        List.of(
            new TestCase("first param (i32)", List.of(R.I32), 0, R.I32),
            new TestCase("second param (i64)", List.of(R.I32, R.I64), 1, R.I64),
            new TestCase("third param (string)", List.of(R.I32, R.I64, R.STRING), 2, R.STRING),
            new TestCase("float64 param", List.of(R.FP64), 0, R.FP64),
            new TestCase("date param", List.of(R.BOOLEAN, R.DATE, R.TIMESTAMP), 1, R.DATE));

    for (TestCase tc : testCases) {
      Expression.Lambda lambda = lb.lambda(tc.paramTypes, params -> params.ref(tc.fieldIndex));

      verifyRoundTrip(lambda);

      assertEquals(tc.expectedType, lambda.body().getType(), tc.name + ": body type mismatch");
      Type.Func funcType = (Type.Func) lambda.getType();
      assertEquals(
          tc.expectedType, funcType.returnType(), tc.name + ": lambda return type mismatch");
    }
  }

  // (x: i32, y: string) -> y — verify full Func type structure
  @Test
  void lambdaGetTypeReturnsFunc() {
    Expression.Lambda lambda = lb.lambda(List.of(R.I32, R.STRING), params -> params.ref(1));

    Type.Func funcType = (Type.Func) lambda.getType();
    assertEquals(2, funcType.parameterTypes().size());
    assertEquals(R.I32, funcType.parameterTypes().get(0));
    assertEquals(R.STRING, funcType.parameterTypes().get(1));
    assertEquals(R.STRING, funcType.returnType());
  }

  // (x: i32, y: i64, z: string) -> ... — verify FieldReference metadata for each param
  @Test
  void parameterReferenceMetadata() {
    List<Type> paramTypes = List.of(R.I32, R.I64, R.STRING);

    lb.lambda(
        paramTypes,
        params -> {
          for (int i = 0; i < 3; i++) {
            FieldReference ref = params.ref(i);
            assertTrue(ref.isLambdaParameterReference());
            assertFalse(ref.isOuterReference());
            assertFalse(ref.isSimpleRootReference());
            assertEquals(0, ref.lambdaParameterReferenceStepsOut().orElse(-1));
            assertEquals(paramTypes.get(i), ref.getType());
          }
          return params.ref(0);
        });
  }

  // ==================== Expression Body Tests ====================

  // (x: i64) -> (y1: i64, y2: i64) -> y1 * x + y2
  @Test
  void nestedLambdaWithArithmeticBody() {
    String ARITH = DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC;

    Expression.Lambda result =
        lb.lambda(
            List.of(R.I64),
            outer ->
                lb.lambda(
                    List.of(R.I64, R.I64),
                    inner -> {
                      // y1 * x
                      Expression multiply =
                          sb.scalarFn(ARITH, "multiply:i64_i64", R.I64, inner.ref(0), outer.ref(0));
                      // (y1 * x) + y2
                      return sb.scalarFn(ARITH, "add:i64_i64", R.I64, multiply, inner.ref(1));
                    }));

    verifyRoundTrip(result);

    // Outer lambda returns a func type
    Type.Func outerFuncType = (Type.Func) result.getType();
    assertInstanceOf(Type.Func.class, outerFuncType.returnType());

    // Inner lambda returns i64
    Expression.Lambda innerLambda = (Expression.Lambda) result.body();
    Type.Func innerFuncType = (Type.Func) innerLambda.getType();
    assertEquals(R.I64, innerFuncType.returnType());

    // Inner body is a scalar function (add)
    assertInstanceOf(Expression.ScalarFunctionInvocation.class, innerLambda.body());
  }

  // ==================== Nested Lambda Tests ====================

  // (x: i64, y: i64) -> (z: i32) -> x
  @Test
  void nestedLambdaWithOuterRef() {
    Expression.Lambda result =
        lb.lambda(List.of(R.I64, R.I64), outer -> lb.lambda(List.of(R.I32), inner -> outer.ref(0)));

    verifyRoundTrip(result);

    Expression.Lambda resultInner = (Expression.Lambda) result.body();
    assertEquals(1, resultInner.parameters().fields().size());
    assertEquals(R.I64, resultInner.body().getType());
  }

  // (x: i32, y: i64, z: string) -> (w: fp64) -> z
  @Test
  void nestedLambdaOuterRefTypeResolution() {
    Expression.Lambda result =
        lb.lambda(
            List.of(R.I32, R.I64, R.STRING),
            outer -> lb.lambda(List.of(R.FP64), inner -> outer.ref(2)));

    verifyRoundTrip(result);

    Expression.Lambda resultInner = (Expression.Lambda) result.body();
    assertEquals(R.STRING, ((Type.Func) resultInner.getType()).returnType());
  }

  // (x: i32) -> (y: i64) -> y
  @Test
  void nestedLambdaInnerRefOnly() {
    Expression.Lambda result =
        lb.lambda(List.of(R.I32), outer -> lb.lambda(List.of(R.I64), inner -> inner.ref(0)));

    verifyRoundTrip(result);

    Expression.Lambda innerLambda = (Expression.Lambda) result.body();
    assertEquals(R.I64, innerLambda.body().getType());
    assertInstanceOf(Type.Func.class, ((Type.Func) result.getType()).returnType());
  }

  // (x: i32) -> (y: i64) -> (x, y) — body references both outer and inner params
  @Test
  void nestedLambdaBothInnerAndOuterRefs() {
    Expression.Lambda result =
        lb.lambda(
            List.of(R.I32),
            outer ->
                lb.lambda(
                    List.of(R.I64),
                    inner -> {
                      FieldReference innerRef = inner.ref(0);
                      assertEquals(R.I64, innerRef.getType());
                      assertEquals(0, innerRef.lambdaParameterReferenceStepsOut().orElse(-1));

                      FieldReference outerRef = outer.ref(0);
                      assertEquals(R.I32, outerRef.getType());
                      assertEquals(1, outerRef.lambdaParameterReferenceStepsOut().orElse(-1));

                      return innerRef;
                    }));

    verifyRoundTrip(result);
  }

  // (a: i32, b: string) -> (c: i64, d: fp64) -> b — verify all 4 params resolve correctly
  @Test
  void nestedLambdaMultiParamCorrectResolution() {
    Expression.Lambda result =
        lb.lambda(
            List.of(R.I32, R.STRING),
            outer ->
                lb.lambda(
                    List.of(R.I64, R.FP64),
                    inner -> {
                      assertEquals(R.I64, inner.ref(0).getType());
                      assertEquals(R.FP64, inner.ref(1).getType());
                      assertEquals(R.I32, outer.ref(0).getType());
                      assertEquals(R.STRING, outer.ref(1).getType());

                      return outer.ref(1);
                    }));

    verifyRoundTrip(result);

    Expression.Lambda innerLambda = (Expression.Lambda) result.body();
    assertEquals(R.STRING, ((Type.Func) innerLambda.getType()).returnType());
  }

  // (x: i32) -> (y: i64) -> (z: string) -> x
  @Test
  void tripleNestedLambdaRoundtrip() {
    Expression.Lambda result =
        lb.lambda(
            List.of(R.I32),
            outer ->
                lb.lambda(
                    List.of(R.I64), mid -> lb.lambda(List.of(R.STRING), inner -> outer.ref(0))));

    verifyRoundTrip(result);

    Expression.Lambda l1 = (Expression.Lambda) result.body();
    Expression.Lambda l2 = (Expression.Lambda) l1.body();
    assertEquals(R.I32, l2.body().getType());
  }

  // (x: i32) -> (y: i64) -> (z: string) -> ... — verify stepsOut is auto-computed at each level
  @Test
  void tripleNestedLambdaScopeTracking() {
    lb.lambda(
        List.of(R.I32),
        outer ->
            lb.lambda(
                List.of(R.I64),
                mid ->
                    lb.lambda(
                        List.of(R.STRING),
                        inner -> {
                          assertEquals(R.STRING, inner.ref(0).getType());
                          assertEquals(R.I64, mid.ref(0).getType());
                          assertEquals(R.I32, outer.ref(0).getType());

                          assertEquals(
                              0, inner.ref(0).lambdaParameterReferenceStepsOut().orElse(-1));
                          assertEquals(1, mid.ref(0).lambdaParameterReferenceStepsOut().orElse(-1));
                          assertEquals(
                              2, outer.ref(0).lambdaParameterReferenceStepsOut().orElse(-1));

                          return inner.ref(0);
                        })));
  }
}
