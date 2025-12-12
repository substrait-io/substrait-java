package io.substrait.isthmus;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import org.junit.jupiter.api.Test;

public class ProjectTest extends PlanTestBase {
  final SubstraitBuilder b = new SubstraitBuilder(extensions);
  final Rel emptyTable = b.emptyScan();

  @Test
  void avoidProjectRemapOnEmptyInput() {
    Rel projection =
        Project.builder().input(emptyTable).addExpressions(b.add(b.i32(1), b.i32(2))).build();
    assertFullRoundTrip(projection);
  }
}
