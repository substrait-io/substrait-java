package io.substrait.isthmus;

import io.substrait.relation.Rel;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.junit.jupiter.api.Test;

class FetchTest extends PlanTestBase {

  static final TypeCreator R = TypeCreator.of(false);

  final Rel TABLE = sb.namedScan(List.of("test"), List.of("col1"), List.of(R.STRING));

  @Test
  void limitOnly() {
    Rel rel = sb.limit(50, TABLE);
    assertFullRoundTrip(rel);
  }

  @Test
  void offsetOnly() {
    Rel rel = sb.offset(50, TABLE);
    assertFullRoundTrip(rel);
  }

  @Test
  void offsetAndLimit() {
    Rel rel = sb.fetch(50, 10, TABLE);
    assertFullRoundTrip(rel);
  }
}
