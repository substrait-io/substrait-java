package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableFieldReference;
import io.substrait.expression.proto.ProtoExpressionConverter;
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
   * The two outer-reference forms map to a single protobuf {@code oneof} and are therefore mutually
   * exclusive: a {@link FieldReference} may not carry both at once.
   */
  @Test
  void outerReferenceFormsAreMutuallyExclusive() {
    ImmutableFieldReference.Builder both =
        ImmutableFieldReference.builder()
            .from(FieldReference.newRootStructOuterReference(0, outerSchema, 3))
            .outerReferenceRelReference(42);

    assertThrows(IllegalArgumentException.class, both::build);
  }
}
