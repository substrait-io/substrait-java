package io.substrait.relation;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.TestBase;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/**
 * Validation tests for {@link Fetch}, whose offset/count expressions must have an integer type.
 * Round-trip coverage lives in the {@code io.substrait.type.proto} package.
 */
class FetchTest extends TestBase {

  // Reuse the same schema shape as FetchRoundtripTest so the two files stay in sync.
  final Rel table =
      sb.namedScan(Arrays.asList("T"), Arrays.asList("a", "b"), Arrays.asList(R.I64, R.STRING));

  /** Every integer width is an acceptable offset/count expression type. */
  @Test
  void integerWidthsAccepted() {
    fetch().offset(sb.i8(1)).count(sb.i8(2)).build();
    fetch().offset(sb.i16(1)).count(sb.i16(2)).build();
    fetch().offset(sb.i32(1)).count(sb.i32(2)).build();
    fetch().offset(sb.i64(1)).count(sb.i64(2)).build();
  }

  /** A non-integer offset expression is rejected at construction time. */
  @Test
  void nonIntegerOffsetRejected() {
    assertThrows(IllegalArgumentException.class, () -> fetch().offset(sb.fp64(1.0)).build());
  }

  /** A non-integer count expression is rejected at construction time. */
  @Test
  void nonIntegerCountRejected() {
    assertThrows(IllegalArgumentException.class, () -> fetch().count(sb.fp64(1.0)).build());
  }

  /** A non-integer type reaches the check via the proto conversion path too. */
  @Test
  void nonIntegerOffsetRejectedViaProto() {
    // Build a FetchRel proto directly with a non-integer offset_expr so the Fetch POJO check is
    // exercised by ProtoRelConverter rather than by direct construction.
    io.substrait.proto.Rel inputProto = relProtoConverter.toProto(table);
    io.substrait.proto.Expression fp64Expr =
        io.substrait.proto.Expression.newBuilder()
            .setLiteral(io.substrait.proto.Expression.Literal.newBuilder().setFp64(1.0).build())
            .build();
    io.substrait.proto.FetchRel fetchRel =
        io.substrait.proto.FetchRel.newBuilder()
            .setCommon(io.substrait.proto.RelCommon.newBuilder().build())
            .setInput(inputProto)
            .setOffsetExpr(fp64Expr)
            .build();
    io.substrait.proto.Rel protoRel =
        io.substrait.proto.Rel.newBuilder().setFetch(fetchRel).build();
    assertThrows(IllegalArgumentException.class, () -> protoRelConverter.from(protoRel));
  }

  private ImmutableFetch.Builder fetch() {
    return Fetch.builder().input(table);
  }
}
