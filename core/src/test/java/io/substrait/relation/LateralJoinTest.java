package io.substrait.relation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.TestBase;
import io.substrait.util.EmptyVisitationContext;
import java.util.Arrays;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class LateralJoinTest extends TestBase {

  final Rel leftTable =
      sb.namedScan(
          Arrays.asList("T1"),
          Arrays.asList("a", "b", "c"),
          Arrays.asList(R.I64, R.FP64, R.STRING));

  final Rel rightTable =
      sb.namedScan(
          Arrays.asList("T2"),
          Arrays.asList("d", "e", "f"),
          Arrays.asList(R.FP64, R.STRING, R.I64));

  @Test
  void rejectsNonLateralJoinType() {
    // The spec forbids RIGHT-oriented and OUTER join types for lateral joins.
    ImmutableLateralJoin.Builder builder =
        LateralJoin.builder()
            .left(leftTable)
            .right(rightTable)
            .joinType(Join.JoinType.RIGHT)
            .relAnchor(1);
    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void requiresRelAnchor() {
    // A lateral join must carry a rel anchor for the right input to reference the left row.
    ImmutableLateralJoin.Builder builder =
        LateralJoin.builder().left(leftTable).right(rightTable).joinType(Join.JoinType.INNER);
    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void copyOnWritePreservesLateralJoinFields() {
    LateralJoin lateralJoin =
        LateralJoin.builder()
            .left(leftTable)
            .right(rightTable)
            .condition(
                sb.equal(
                    sb.fieldReference(Arrays.asList(leftTable, rightTable), 0),
                    sb.fieldReference(Arrays.asList(leftTable, rightTable), 5)))
            .postJoinFilter(
                sb.equal(
                    sb.fieldReference(Arrays.asList(leftTable, rightTable), 2),
                    sb.fieldReference(Arrays.asList(leftTable, rightTable), 4)))
            .joinType(Join.JoinType.LEFT)
            .relAnchor(7)
            .build();

    // Same schema, different table name, so the rewritten join stays valid.
    final Rel replacementLeft =
        sb.namedScan(
            Arrays.asList("T1_MODIFIED"),
            Arrays.asList("a", "b", "c"),
            Arrays.asList(R.I64, R.FP64, R.STRING));

    RelCopyOnWriteVisitor<RuntimeException> visitor =
        new RelCopyOnWriteVisitor<RuntimeException>() {
          @Override
          public Optional<Rel> visit(NamedScan namedScan, EmptyVisitationContext context) {
            return namedScan.equals(leftTable) ? Optional.of(replacementLeft) : Optional.empty();
          }
        };

    Optional<Rel> result = lateralJoin.accept(visitor, EmptyVisitationContext.INSTANCE);

    assertTrue(result.isPresent());
    LateralJoin rewritten = (LateralJoin) result.get();
    assertEquals(replacementLeft, rewritten.getLeft());
    assertEquals(rightTable, rewritten.getRight());
    assertEquals(Join.JoinType.LEFT, rewritten.getJoinType());
    assertEquals(Optional.of(7), rewritten.getRelAnchor());
    assertEquals(lateralJoin.getCondition(), rewritten.getCondition());
    // The rewrite must preserve the post-join filter, not drop it or alias it to the condition.
    assertEquals(lateralJoin.getPostJoinFilter(), rewritten.getPostJoinFilter());
    assertTrue(rewritten.getPostJoinFilter().isPresent());
  }
}
