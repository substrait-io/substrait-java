package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.utils.RelSamples;
import io.substrait.utils.StringHolder;
import io.substrait.utils.StringHolderHandlingProtoRelConverter;
import io.substrait.utils.StringHolderHandlingRelProtoConverter;

/**
 * A {@link TestBase} that round-trips {@link StringHolder} extension details, as the shared {@link
 * RelSamples} carry.
 *
 * <p>The handling reader routes <em>every</em> detail through {@link StringHolder#fromProto}, which
 * throws on an {@code Any} holding anything else, so a consumer of those samples cannot mix the
 * plain converters back in. Advanced extensions are unaffected either way: the extension converters
 * only reach for a detail handler when an enhancement or optimization is actually present.
 */
abstract class StringHolderRoundtripTestBase extends TestBase {

  /** Shares {@code TestBase}'s collector, which allocates the function anchors the reader needs. */
  private final RelProtoConverter stringHolderRelProtoConverter =
      new StringHolderHandlingRelProtoConverter(functionCollector);

  @Override
  protected void verifyRoundTrip(Rel rel) {
    io.substrait.proto.Rel protoRel = stringHolderRelProtoConverter.toProto(rel);
    // A fresh reader per round trip: a ProtoRelConverter accumulates the outer-reference and anchor
    // scopes it descends through, and the test instance is shared across every sample.
    Rel relReturned =
        new StringHolderHandlingProtoRelConverter(functionCollector, extensions).from(protoRel);
    assertEquals(rel, relReturned);
  }
}
