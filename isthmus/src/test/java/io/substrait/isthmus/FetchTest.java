package io.substrait.isthmus;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.relation.Rel;
import io.substrait.type.TypeCreator;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class FetchTest extends PlanTestBase {

  static final TypeCreator R = TypeCreator.of(false);

  final SubstraitBuilder b = new SubstraitBuilder(extensions);

  final Rel TABLE =
      b.namedScan(Arrays.asList("test"), Arrays.asList("col1"), Arrays.asList(R.STRING));

  @Test
  void limitOnly() {
    Rel rel = b.limit(50, TABLE);
    assertFullRoundTrip(rel);
  }

  @Test
  void offsetOnly() {
    Rel rel = b.offset(50, TABLE);
    assertFullRoundTrip(rel);
  }

  @Test
  void offsetAndLimit() {
    Rel rel = b.fetch(50, 10, TABLE);
    assertFullRoundTrip(rel);
  }
}
