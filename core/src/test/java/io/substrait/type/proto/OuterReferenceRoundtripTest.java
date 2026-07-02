package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableFieldReference;
import io.substrait.expression.proto.ProtoExpressionConverter;
import io.substrait.proto.Expression.FieldReference.OuterReference.OuterReferenceTypeCase;
import io.substrait.type.Type;
import org.junit.jupiter.api.Test;

/**
 * Round-trip tests for the two outer-reference resolution mechanisms introduced with Substrait
 * v0.89.0: the offset-based {@link FieldReference#outerReferenceStepsOut()} and the id-based {@link
 * FieldReference#outerReferenceRelReference()}.
 *
 * <p>Outer references derive their type from the converter's root type on the way back from proto,
 * so a converter configured with a matching root type is used instead of {@link
 * TestBase#protoExpressionConverter}.
 */
class OuterReferenceRoundtripTest extends TestBase {

  final Type.Struct outerSchema =
      Type.Struct.builder().nullable(false).addFields(R.I64, R.STRING).build();

  final ProtoExpressionConverter protoExpressionConverterWithRoot =
      new ProtoExpressionConverter(functionCollector, extensions, outerSchema, protoRelConverter);

  private void verifyOuterReferenceRoundTrip(FieldReference reference) {
    io.substrait.proto.Expression proto = expressionProtoConverter.toProto(reference);
    Expression returned = protoExpressionConverterWithRoot.from(proto);
    assertEquals(reference, returned);
  }

  @Test
  void offsetBasedOuterReference() {
    FieldReference reference = FieldReference.newRootStructOuterReference(0, outerSchema, 1);

    assertTrue(reference.isOuterReference());
    assertFalse(reference.isSimpleRootReference());
    verifyOuterReferenceRoundTrip(reference);
  }

  @Test
  void idBasedOuterReference() {
    FieldReference reference =
        FieldReference.newRootStructOuterReferenceByRelReference(1, outerSchema, 42);

    assertTrue(reference.isOuterReference());
    assertFalse(reference.isSimpleRootReference());
    verifyOuterReferenceRoundTrip(reference);
  }

  /**
   * A reference may carry both outer-reference forms during the transition towards id-based
   * resolution. Producing prefers the id-based {@code rel_reference}; since the two share a
   * protobuf oneof, the offset-based {@code steps_out} is not emitted and is therefore absent after
   * a round-trip.
   */
  @Test
  void bothFormsSetPrefersIdBased() {
    FieldReference reference =
        ImmutableFieldReference.builder()
            .from(FieldReference.newRootStructOuterReference(0, outerSchema, 3))
            .outerReferenceRelReference(42)
            .build();

    assertTrue(reference.outerReferenceStepsOut().isPresent());
    assertTrue(reference.outerReferenceRelReference().isPresent());

    io.substrait.proto.Expression proto = expressionProtoConverter.toProto(reference);
    io.substrait.proto.Expression.FieldReference.OuterReference outerReference =
        proto.getSelection().getOuterReference();
    assertEquals(OuterReferenceTypeCase.REL_REFERENCE, outerReference.getOuterReferenceTypeCase());
    assertEquals(42, outerReference.getRelReference());

    FieldReference returned = (FieldReference) protoExpressionConverterWithRoot.from(proto);
    assertEquals(42, returned.outerReferenceRelReference().orElseThrow(AssertionError::new));
    assertFalse(returned.outerReferenceStepsOut().isPresent());
  }
}
