package io.substrait.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.type.Type;
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
        reference, reference.dereferenceStruct(0), R.BOOLEAN, FieldReference.StructField.of(0));
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
    return switch (scope) {
      case ROOT -> FieldReference.newRootStructReference(1, type);
      case EXPRESSION ->
          FieldReference.newStructReference(
              1,
              Expression.DynamicParameter.builder()
                  .type(R.struct(R.BOOLEAN, type))
                  .parameterReference(0)
                  .build());
      case OUTER_STEPS -> FieldReference.newRootStructOuterReference(1, type, 2);
      case OUTER_ANCHOR -> FieldReference.newRootStructOuterReferenceByRelReference(1, type, 7);
      case LAMBDA_CURRENT -> FieldReference.newLambdaParameterReference(0, 1, type);
      case LAMBDA_OUTER -> FieldReference.newLambdaParameterReference(2, 1, type);
    };
  }

  private void assertDereference(
      FieldReference original,
      FieldReference dereferenced,
      Type expectedType,
      FieldReference.ReferenceSegment nextSegment) {
    assertEquals(
        ImmutableFieldReference.copyOf(original)
            .withType(expectedType)
            .withSegments(nextSegment, original.segments().get(0)),
        dereferenced);

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
