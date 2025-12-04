package io.substrait.isthmus;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression.EmptyListLiteral;
import io.substrait.expression.ExpressionCreator;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.junit.jupiter.api.Test;

class EmptyArrayLiteralTest extends PlanTestBase {
  private static final TypeCreator N = TypeCreator.of(true);

  private final SubstraitBuilder b = new SubstraitBuilder(extensions);

  @Test
  void emptyArrayLiteral() {
    Type colType = N.I8;
    EmptyListLiteral emptyListLiteral = ExpressionCreator.emptyList(false, N.I8);
    Project rel =
        b.project(
            input -> List.of(emptyListLiteral),
            Rel.Remap.offset(1, 1),
            b.namedScan(List.of("t"), List.of("col"), List.of(colType)));
    assertFullRoundTrip(rel);
  }

  @Test
  void nullableEmptyArrayLiteral() {
    Type colType = N.I8;
    EmptyListLiteral emptyListLiteral = ExpressionCreator.emptyList(true, N.I8);
    Project rel =
        b.project(
            input -> List.of(emptyListLiteral),
            Rel.Remap.offset(1, 1),
            b.namedScan(List.of("t"), List.of("col"), List.of(colType)));
    assertFullRoundTrip(rel);
  }
}
