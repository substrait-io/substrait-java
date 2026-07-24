package io.substrait.type.proto;

import io.substrait.TestBase;
import io.substrait.relation.Fetch;
import io.substrait.relation.Rel;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/**
 * Round-trip tests for {@link Fetch}, whose offset/count are optional expressions (an unset offset
 * is treated as 0, an unset count as LIMIT ALL).
 */
class FetchRoundtripTest extends TestBase {

  final Rel table =
      sb.namedScan(Arrays.asList("T"), Arrays.asList("a", "b"), Arrays.asList(R.I64, R.STRING));

  @Test
  void limitOnly() {
    verifyRoundTrip(sb.limit(10, table));
  }

  @Test
  void offsetOnly() {
    verifyRoundTrip(sb.offset(5, table));
  }

  @Test
  void offsetAndCount() {
    verifyRoundTrip(sb.fetch(5, 10, table));
  }

  @Test
  void expressionOffsetAndCount() {
    Rel rel = Fetch.builder().input(table).offset(sb.i64(3)).count(sb.i64(7)).build();
    verifyRoundTrip(rel);
  }
}
