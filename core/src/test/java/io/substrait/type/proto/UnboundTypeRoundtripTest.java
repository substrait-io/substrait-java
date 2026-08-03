package io.substrait.type.proto;

import io.substrait.TestBase;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Verifies that a partially bound plan — one whose schema carries the unbound type — survives a
 * POJO &rarr; proto &rarr; POJO round trip at the relation level, exercising the unbound type in
 * its intended position (a field type within a read relation's schema) rather than only as a bare
 * {@link Type}.
 */
class UnboundTypeRoundtripTest extends TestBase {

  @Test
  @DisplayName("named scan whose schema mixes bound and unbound field types round trips")
  void namedScanWithUnboundField() {
    List<Type> fieldTypes = Arrays.asList(R.I64, Type.Unbound.builder().build());
    Rel rel =
        sb.namedScan(
            Arrays.asList("partially_bound_table"),
            Arrays.asList("bound_col", "unbound_col"),
            fieldTypes);
    verifyRoundTrip(rel);
  }

  @Test
  @DisplayName("named scan whose schema is entirely unbound round trips")
  void namedScanWithOnlyUnboundFields() {
    List<Type> fieldTypes =
        Arrays.asList(Type.Unbound.builder().build(), Type.Unbound.builder().build());
    Rel rel =
        sb.namedScan(Arrays.asList("unbound_table"), Arrays.asList("col_a", "col_b"), fieldTypes);
    verifyRoundTrip(rel);
  }
}
