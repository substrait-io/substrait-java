package io.substrait.isthmus;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.relation.Rel;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.junit.jupiter.api.Test;

class FetchTest extends PlanTestBase {

  static final TypeCreator R = TypeCreator.of(false);

  final SubstraitBuilder b = new SubstraitBuilder(extensions);

  final Rel TABLE = b.namedScan(List.of("test"), List.of("col1"), List.of(R.STRING));

  @Test
  void limitOnly() {
    final Rel rel = b.limit(50, TABLE);
    assertFullRoundTrip(rel);
  }

  @Test
  void offsetOnly() {
    final Rel rel = b.offset(50, TABLE);
    assertFullRoundTrip(rel);
  }

  @Test
  void offsetAndLimit() {
    final Rel rel = b.fetch(50, 10, TABLE);
    assertFullRoundTrip(rel);
  }
}
