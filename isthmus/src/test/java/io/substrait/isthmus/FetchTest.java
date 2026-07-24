package io.substrait.isthmus;

import io.substrait.expression.Expression;
import io.substrait.relation.Rel;
import java.util.List;
import org.junit.jupiter.api.Test;

class FetchTest extends PlanTestBase {

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

  @Test
  void limitWithDynamicParameterCount() {
    // Substrait models fetch offset/count as expressions, so a non-literal (dynamic-parameter)
    // count must pass through to Calcite as a RexDynamicParam and back.
    Rel rel =
        sb.limit(
            input ->
                Expression.DynamicParameter.builder().type(R.I64).parameterReference(0).build(),
            TABLE);
    assertFullRoundTrip(rel);
  }
}
