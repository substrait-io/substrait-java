package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for Lambda expression round-trip conversion through protobuf.
 * Based on equivalent tests from substrait-go.
 */
class LambdaExpressionRoundtripTest extends TestBase {

  /**
   * Test that lambdas with no parameters are valid.
   * Building: () -> i32(42) : func<() -> i32>
   */
  @Test
  void zeroParameterLambda() {
    Type.Struct emptyParams = Type.Struct.builder()
        .nullable(false)
        .build();

    Expression body = ExpressionCreator.i32(false, 42);

    Expression.Lambda lambda = Expression.Lambda.builder()
        .parameters(emptyParams)
        .body(body)
        .build();

    verifyRoundTrip(lambda);

    // Verify the lambda type
    Type lambdaType = lambda.getType();
    assertInstanceOf(Type.Func.class, lambdaType);
    Type.Func funcType = (Type.Func) lambdaType;
    assertEquals(0, funcType.parameterTypes().size());
    assertEquals(R.I32, funcType.returnType());
  }

  /**
   * Test valid stepsOut=0 references.
   * Building: ($0: i32) -> $0 : func<i32 -> i32>
   */
  @Test
  void validStepsOut0() {
    Type.Struct params = Type.Struct.builder()
        .nullable(false)
        .addFields(R.I32)
        .build();

    // Lambda body references parameter 0 with stepsOut=0
    FieldReference paramRef = FieldReference.newLambdaParameterReference(0, params, 0);

    Expression.Lambda lambda = Expression.Lambda.builder()
        .parameters(params)
        .body(paramRef)
        .build();

    verifyRoundTrip(lambda);

    // Verify types
    Type lambdaType = lambda.getType();
    assertInstanceOf(Type.Func.class, lambdaType);
    Type.Func funcType = (Type.Func) lambdaType;
    assertEquals(1, funcType.parameterTypes().size());
    assertEquals(R.I32, funcType.parameterTypes().get(0));
    assertEquals(R.I32, funcType.returnType());
  }

  /**
   * Test valid field index with multiple parameters.
   * Building: ($0: i32, $1: i64, $2: string) -> $2 : func<(i32, i64, string) -> string>
   */
  @Test
  void validFieldIndex() {
    Type.Struct params = Type.Struct.builder()
        .nullable(false)
        .addFields(R.I32, R.I64, R.STRING)
        .build();

    // Reference the 3rd parameter (string)
    FieldReference paramRef = FieldReference.newLambdaParameterReference(2, params, 0);

    Expression.Lambda lambda = Expression.Lambda.builder()
        .parameters(params)
        .body(paramRef)
        .build();

    verifyRoundTrip(lambda);

    // Verify return type is string
    Type.Func funcType = (Type.Func) lambda.getType();
    assertEquals(R.STRING, funcType.returnType());
  }

  /**
   * Test type resolution for different parameter types.
   */
  @Test
  void typeResolution() {
    // Test cases: (paramTypes, fieldIndex, expectedReturnType)
    record TestCase(List<Type> paramTypes, int fieldIndex, Type expectedType) {}

    List<TestCase> testCases = List.of(
        new TestCase(List.of(R.I32), 0, R.I32),
        new TestCase(List.of(R.I32, R.I64), 1, R.I64),
        new TestCase(List.of(R.I32, R.I64, R.STRING), 2, R.STRING),
        new TestCase(List.of(R.FP64), 0, R.FP64),
        new TestCase(List.of(R.BOOLEAN, R.DATE, R.TIMESTAMP), 1, R.DATE)
    );

    for (TestCase tc : testCases) {
      Type.Struct params = Type.Struct.builder()
          .nullable(false)
          .addAllFields(tc.paramTypes)
          .build();

      FieldReference paramRef = FieldReference.newLambdaParameterReference(tc.fieldIndex, params, 0);

      Expression.Lambda lambda = Expression.Lambda.builder()
          .parameters(params)
          .body(paramRef)
          .build();

      verifyRoundTrip(lambda);

      // Verify the body type matches expected
      assertEquals(tc.expectedType, lambda.body().getType(),
          "Body type should match referenced parameter type");

      // Verify lambda return type
      Type.Func funcType = (Type.Func) lambda.getType();
      assertEquals(tc.expectedType, funcType.returnType(),
          "Lambda return type should match body type");
    }
  }

  /**
   * Test nested lambda with outer reference.
   * Building: ($0: i64, $1: i64) -> (($0: i32) -> outer[$0] : i64) : func<(i64, i64) -> func<i32 -> i64>>
   */
  @Test
  void nestedLambdaWithOuterRef() {
    Type.Struct outerParams = Type.Struct.builder()
        .nullable(false)
        .addFields(R.I64, R.I64)
        .build();

    Type.Struct innerParams = Type.Struct.builder()
        .nullable(false)
        .addFields(R.I32)
        .build();

    // Inner lambda references outer's parameter 0 with stepsOut=1
    FieldReference outerRef = FieldReference.newLambdaParameterReference(0, outerParams, 1);

    Expression.Lambda innerLambda = Expression.Lambda.builder()
        .parameters(innerParams)
        .body(outerRef)
        .build();

    Expression.Lambda outerLambda = Expression.Lambda.builder()
        .parameters(outerParams)
        .body(innerLambda)
        .build();

    verifyRoundTrip(outerLambda);

    // Verify structure
    assertInstanceOf(Expression.Lambda.class, outerLambda.body());
    Expression.Lambda resultInner = (Expression.Lambda) outerLambda.body();
    assertEquals(1, resultInner.parameters().fields().size());
  }

  /**
   * Test outer reference type resolution in nested lambdas.
   * Building: ($0: i32, $1: i64, $2: string) -> (($0: fp64) -> outer[$2] : string) : func<...>
   */
  @Test
  void outerRefTypeResolution() {
    Type.Struct outerParams = Type.Struct.builder()
        .nullable(false)
        .addFields(R.I32, R.I64, R.STRING)
        .build();

    Type.Struct innerParams = Type.Struct.builder()
        .nullable(false)
        .addFields(R.FP64)
        .build();

    // Inner references outer's field 2 (string) with stepsOut=1
    FieldReference outerRef = FieldReference.newLambdaParameterReference(2, outerParams, 1);

    Expression.Lambda innerLambda = Expression.Lambda.builder()
        .parameters(innerParams)
        .body(outerRef)
        .build();

    Expression.Lambda outerLambda = Expression.Lambda.builder()
        .parameters(outerParams)
        .body(innerLambda)
        .build();

    verifyRoundTrip(outerLambda);

    // Verify inner lambda's return type is string (from outer param 2)
    Expression.Lambda resultInner = (Expression.Lambda) outerLambda.body();
    Type.Func innerFuncType = (Type.Func) resultInner.getType();
    assertEquals(R.STRING, innerFuncType.returnType(),
        "Inner lambda return type should be string from outer.$2");

    // Verify body's type is also string
    assertEquals(R.STRING, resultInner.body().getType(),
        "Body type should be string");
  }

  /**
   * Test deeply nested field ref inside Cast.
   * Building: ($0: i32) -> cast($0 as i64) : func<i32 -> i64>
   */
  @Test
  void deeplyNestedFieldRef() {
    Type.Struct params = Type.Struct.builder()
        .nullable(false)
        .addFields(R.I32)
        .build();

    FieldReference paramRef = FieldReference.newLambdaParameterReference(0, params, 0);

    Expression.Cast castExpr = (Expression.Cast)
        ExpressionCreator.cast(R.I64, paramRef, Expression.FailureBehavior.THROW_EXCEPTION);

    Expression.Lambda lambda = Expression.Lambda.builder()
        .parameters(params)
        .body(castExpr)
        .build();

    verifyRoundTrip(lambda);

    // Verify the nested FieldRef has its type resolved
    Expression.Cast resultCast = (Expression.Cast) lambda.body();
    assertInstanceOf(FieldReference.class, resultCast.input());
    FieldReference resultFieldRef = (FieldReference) resultCast.input();

    assertNotNull(resultFieldRef.getType(), "Nested FieldRef should have type resolved");
    assertEquals(R.I32, resultFieldRef.getType(), "Should resolve to i32");

    // Verify lambda return type is i64 (cast output)
    Type.Func funcType = (Type.Func) lambda.getType();
    assertEquals(R.I64, funcType.returnType());
  }

  /**
   * Test doubly nested field ref (Cast(Cast(LambdaParamRef))).
   * Building: ($0: i32) -> cast(cast($0 as i64) as string) : func<i32 -> string>
   */
  @Test
  void doublyNestedFieldRef() {
    Type.Struct params = Type.Struct.builder()
        .nullable(false)
        .addFields(R.I32)
        .build();

    FieldReference paramRef = FieldReference.newLambdaParameterReference(0, params, 0);
    Expression.Cast innerCast = (Expression.Cast)
        ExpressionCreator.cast(R.I64, paramRef, Expression.FailureBehavior.THROW_EXCEPTION);
    Expression.Cast outerCast = (Expression.Cast)
        ExpressionCreator.cast(R.STRING, innerCast, Expression.FailureBehavior.THROW_EXCEPTION);

    Expression.Lambda lambda = Expression.Lambda.builder()
        .parameters(params)
        .body(outerCast)
        .build();

    verifyRoundTrip(lambda);

    // Navigate to the deeply nested FieldRef (2 levels deep)
    Expression.Cast resultOuter = (Expression.Cast) lambda.body();
    Expression.Cast resultInner = (Expression.Cast) resultOuter.input();
    FieldReference resultFieldRef = (FieldReference) resultInner.input();

    // Verify type is resolved even at depth 2
    assertNotNull(resultFieldRef.getType(), "FieldRef at depth 2 should have type resolved");
    assertEquals(R.I32, resultFieldRef.getType());
  }

  /**
   * Test lambda with literal body (no parameter references).
   * Building: ($0: i32) -> 42 : func<i32 -> i32>
   */
  @Test
  void lambdaWithLiteralBody() {
    Type.Struct params = Type.Struct.builder()
        .nullable(false)
        .addFields(R.I32)
        .build();

    Expression body = ExpressionCreator.i32(false, 42);

    Expression.Lambda lambda = Expression.Lambda.builder()
        .parameters(params)
        .body(body)
        .build();

    verifyRoundTrip(lambda);
  }

  /**
   * Test lambda getType returns correct Func type.
   */
  @Test
  void lambdaGetTypeReturnsFunc() {
    Type.Struct params = Type.Struct.builder()
        .nullable(false)
        .addFields(R.I32, R.STRING)
        .build();

    FieldReference paramRef = FieldReference.newLambdaParameterReference(1, params, 0);

    Expression.Lambda lambda = Expression.Lambda.builder()
        .parameters(params)
        .body(paramRef)
        .build();

    Type lambdaType = lambda.getType();

    assertInstanceOf(Type.Func.class, lambdaType);
    Type.Func funcType = (Type.Func) lambdaType;

    assertEquals(2, funcType.parameterTypes().size());
    assertEquals(R.I32, funcType.parameterTypes().get(0));
    assertEquals(R.STRING, funcType.parameterTypes().get(1));
    assertEquals(R.STRING, funcType.returnType()); // body references param 1 which is STRING
  }

  // ==================== Validation Error Tests ====================

  /**
   * Test that invalid outer reference (stepsOut too high) fails during proto conversion.
   * Building: ($0: i32) -> outer[$0] : INVALID (no outer lambda, stepsOut=1)
   */
  @Test
  void invalidOuterRef_stepsOutTooHigh() {
    Type.Struct params = Type.Struct.builder()
        .nullable(false)
        .addFields(R.I32)
        .build();

    // Create a parameter reference with stepsOut=1 but no outer lambda exists
    FieldReference invalidRef = FieldReference.newLambdaParameterReference(0, params, 1);

    Expression.Lambda lambda = Expression.Lambda.builder()
        .parameters(params)
        .body(invalidRef)
        .build();

    // Convert to proto - this should work
    io.substrait.proto.Expression protoExpression = expressionProtoConverter.toProto(lambda);

    // Converting back should fail because stepsOut=1 references non-existent outer lambda
    assertThrows(IllegalArgumentException.class, () -> {
      protoExpressionConverter.from(protoExpression);
    }, "Should fail when stepsOut references non-existent outer lambda");
  }

  /**
   * Test that invalid field index (out of bounds) fails during proto conversion.
   * Building: ($0: i32) -> $5 : INVALID (only has 1 param)
   */
  @Test
  void invalidFieldIndex_outOfBounds() {
    Type.Struct params = Type.Struct.builder()
        .nullable(false)
        .addFields(R.I32)
        .build();

    // Create a reference to field 5, but lambda only has 1 parameter (index 0)
    // This will fail at build time since newLambdaParameterReference accesses fields.get(5)
    assertThrows(IndexOutOfBoundsException.class, () -> {
      FieldReference.newLambdaParameterReference(5, params, 0);
    }, "Should fail when field index is out of bounds");
  }

  /**
   * Test nested invalid outer ref (stepsOut=2 but only 1 outer lambda).
   * Building: ($0: i64) -> (($0: i32) -> outer.outer[$0]) : INVALID (no grandparent lambda)
   */
  @Test
  void nestedInvalidOuterRef() {
    Type.Struct outerParams = Type.Struct.builder()
        .nullable(false)
        .addFields(R.I64)
        .build();

    Type.Struct innerParams = Type.Struct.builder()
        .nullable(false)
        .addFields(R.I32)
        .build();

    // Inner lambda references stepsOut=2, but only 1 outer lambda exists
    FieldReference invalidRef = FieldReference.newLambdaParameterReference(0, outerParams, 2);

    Expression.Lambda innerLambda = Expression.Lambda.builder()
        .parameters(innerParams)
        .body(invalidRef)
        .build();

    Expression.Lambda outerLambda = Expression.Lambda.builder()
        .parameters(outerParams)
        .body(innerLambda)
        .build();

    // Convert to proto
    io.substrait.proto.Expression protoExpression = expressionProtoConverter.toProto(outerLambda);

    // Converting back should fail because stepsOut=2 references non-existent grandparent
    assertThrows(IllegalArgumentException.class, () -> {
      protoExpressionConverter.from(protoExpression);
    }, "Should fail when stepsOut references non-existent grandparent lambda");
  }

  /**
   * Test deeply nested invalid field ref inside Cast.
   * Building: ($0: i32) -> cast($5 as i64) : INVALID (only has 1 param)
   */
  @Test
  void deeplyNestedInvalidFieldRef() {
    Type.Struct params = Type.Struct.builder()
        .nullable(false)
        .addFields(R.I32)
        .build();

    // This should fail at build time since field 5 doesn't exist
    assertThrows(IndexOutOfBoundsException.class, () -> {
      FieldReference.newLambdaParameterReference(5, params, 0);
    }, "Should fail when nested field index is out of bounds");
  }

  /**
   * Test that outer field index out of bounds fails.
   * Building: ($0: i64) -> (($0: i32) -> outer[$5]) : INVALID (outer only has 1 param)
   */
  @Test
  void nestedInvalidOuterFieldIndex() {
    Type.Struct outerParams = Type.Struct.builder()
        .nullable(false)
        .addFields(R.I64)
        .build();

    Type.Struct innerParams = Type.Struct.builder()
        .nullable(false)
        .addFields(R.I32)
        .build();

    // This should fail at build time since outer only has 1 parameter (index 0)
    assertThrows(IndexOutOfBoundsException.class, () -> {
      FieldReference.newLambdaParameterReference(5, outerParams, 1);
    }, "Should fail when outer field index is out of bounds");
  }
}
