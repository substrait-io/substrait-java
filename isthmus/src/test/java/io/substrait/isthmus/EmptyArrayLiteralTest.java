package io.substrait.isthmus;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.ExpressionCreator;
import io.substrait.relation.Rel;
import io.substrait.type.TypeCreator;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class EmptyArrayLiteralTest extends PlanTestBase {
  private static final TypeCreator N = TypeCreator.of(true);

  private final SubstraitBuilder b = new SubstraitBuilder(extensions);

  @Test
  void emptyArrayLiteral() {
    var colType = N.I8;
    var emptyListLiteral = ExpressionCreator.emptyList(false, N.I8);
    var rel =
        b.project(
            input -> Arrays.asList(emptyListLiteral),
            Rel.Remap.offset(1, 1),
            b.namedScan(Arrays.asList("t"), Arrays.asList("col"), Arrays.asList(colType)));
    assertFullRoundTrip(rel);
  }

  @Test
  void nullableEmptyArrayLiteral() {
    var colType = N.I8;
    var emptyListLiteral = ExpressionCreator.emptyList(true, N.I8);
    var rel =
        b.project(
            input -> Arrays.asList(emptyListLiteral),
            Rel.Remap.offset(1, 1),
            b.namedScan(Arrays.asList("t"), Arrays.asList("col"), Arrays.asList(colType)));
    assertFullRoundTrip(rel);
  }
}
