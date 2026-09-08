package io.substrait.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.type.Type;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class FieldReferenceDereferenceTest extends TestBase {

  enum ReferenceScope {
    ROOT,
    EXPRESSION,
    OUTER_STEPS,
    OUTER_ANCHOR,
    LAMBDA_CURRENT,
    LAMBDA_OUTER
  }

  @ParameterizedTest
  @EnumSource(ReferenceScope.class)
  void structDereferencePreservesScope(ReferenceScope scope) {
    FieldReference reference = reference(scope, R.struct(R.BOOLEAN, N.I64));

    assertDereference(
        reference, reference.dereferenceStruct(1), N.I64, FieldReference.StructField.of(1));
  }

  @ParameterizedTest
  @EnumSource(ReferenceScope.class)
  void listDereferencePreservesScope(ReferenceScope scope) {
    FieldReference reference = reference(scope, R.list(N.I64));

    assertDereference(
        reference, reference.dereferenceList(2), N.I64, FieldReference.ListElement.of(2));
  }

  @ParameterizedTest
  @EnumSource(ReferenceScope.class)
  void mapDereferencePreservesScope(ReferenceScope scope) {
    FieldReference reference = reference(scope, R.map(R.STRING, N.I64));
    Expression.Literal key = ExpressionCreator.string(false, "key");

    assertDereference(
        reference, reference.dereferenceMap(key), N.I64, FieldReference.MapKey.of(key));
  }

  private FieldReference reference(ReferenceScope scope, Type type) {
    ImmutableFieldReference.Builder builder =
        FieldReference.builder().type(type).addSegments(FieldReference.StructField.of(1));
    switch (scope) {
      case EXPRESSION:
        builder.inputExpression(
            Expression.DynamicParameter.builder()
                .type(R.struct(R.BOOLEAN, type))
                .parameterReference(0)
                .build());
        break;
      case OUTER_STEPS:
        builder.outerReferenceStepsOut(2);
        break;
      case OUTER_ANCHOR:
        builder.outerReferenceRelReference(7);
        break;
      case LAMBDA_CURRENT:
        builder.lambdaParameterReferenceStepsOut(0);
        break;
      case LAMBDA_OUTER:
        builder.lambdaParameterReferenceStepsOut(2);
        break;
      case ROOT:
        break;
      default:
        throw new IllegalArgumentException("Unexpected reference scope: " + scope);
    }
    return builder.build();
  }

  private void assertDereference(
      FieldReference original,
      FieldReference dereferenced,
      Type expectedType,
      FieldReference.ReferenceSegment nextSegment) {
    assertEquals(expectedType, dereferenced.getType());
    assertEquals(List.of(nextSegment, original.segments().get(0)), dereferenced.segments());
    assertEquals(original.inputExpression(), dereferenced.inputExpression());
    assertEquals(original.outerReferenceStepsOut(), dereferenced.outerReferenceStepsOut());
    assertEquals(original.outerReferenceRelReference(), dereferenced.outerReferenceRelReference());
    assertEquals(
        original.lambdaParameterReferenceStepsOut(),
        dereferenced.lambdaParameterReferenceStepsOut());

    io.substrait.proto.Expression.FieldReference originalProto =
        expressionProtoConverter.toProto(original).getSelection();
    io.substrait.proto.Expression.FieldReference dereferencedProto =
        expressionProtoConverter.toProto(dereferenced).getSelection();
    assertEquals(originalProto.getRootTypeCase(), dereferencedProto.getRootTypeCase());
    assertEquals(originalProto.getOuterReference(), dereferencedProto.getOuterReference());
    assertEquals(
        originalProto.getLambdaParameterReference(),
        dereferencedProto.getLambdaParameterReference());
  }
}
