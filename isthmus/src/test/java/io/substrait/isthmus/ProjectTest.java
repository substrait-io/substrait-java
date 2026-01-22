package io.substrait.isthmus;

import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import org.junit.jupiter.api.Test;

class ProjectTest extends PlanTestBase {
  final Rel emptyTable = sb.emptyVirtualTableScan();

  @Test
  void avoidProjectRemapOnEmptyInput() {
    Rel projection =
        Project.builder().input(emptyTable).addExpressions(sb.add(sb.i32(1), sb.i32(2))).build();
    assertFullRoundTrip(projection);
  }
}
