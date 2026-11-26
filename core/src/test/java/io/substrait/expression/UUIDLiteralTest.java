package io.substrait.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.ByteString;
import io.substrait.TestBase;
import java.util.HexFormat;
import java.util.UUID;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class UUIDLiteralTest extends TestBase {

  @ParameterizedTest
  @ValueSource(
      strings = {
        "00112233-4455-6677-8899-aabbccddeeff",
        "00000000-0000-0000-0000-000000000000",
        "ffffffff-ffff-ffff-ffff-ffffffffffff",
        "01234567-89ab-cdef-0123-456789abcdef"
      })
  void bytesFollowNetworkByteOrder(String text) {
    ByteString bytes = ByteString.copyFrom(HexFormat.of().parseHex(text.replace("-", "")));

    Expression.UUIDLiteral literal = ExpressionCreator.uuid(false, bytes);

    assertEquals(UUID.fromString(text), literal.value());
    assertEquals(bytes, literal.toBytes());
    verifyRoundTrip(literal);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 15, 17})
  void protoRejectsWrongLength(int size) {
    io.substrait.proto.Expression proto =
        io.substrait.proto.Expression.newBuilder()
            .setLiteral(
                io.substrait.proto.Expression.Literal.newBuilder()
                    .setUuid(ByteString.copyFrom(new byte[size])))
            .build();
    assertThrows(IllegalArgumentException.class, () -> protoExpressionConverter.from(proto));
  }
}
