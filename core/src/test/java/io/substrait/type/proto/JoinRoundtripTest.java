package io.substrait.type.proto;

import io.substrait.TestBase;
import io.substrait.relation.Join;
import io.substrait.relation.LateralJoin;
import io.substrait.relation.Rel;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.NestedLoopJoin;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class JoinRoundtripTest extends TestBase {

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
  void hashJoin() {
    List<Integer> leftKeys = Arrays.asList(0, 1);
    List<Integer> rightKeys = Arrays.asList(2, 0);
    Rel relWithoutKeys =
        HashJoin.builder()
            .from(sb.hashJoin(leftKeys, rightKeys, HashJoin.JoinType.INNER, leftTable, rightTable))
            .build();
    verifyRoundTrip(relWithoutKeys);
  }

  @Test
  void hashJoinWithResidualExpression() {
    List<Integer> leftKeys = Arrays.asList(0, 1);
    List<Integer> rightKeys = Arrays.asList(2, 0);
    // Residual filter over the combined left+right schema: left.a (I64, index 0) == right.f (I64,
    // index 5).
    Rel rel =
        HashJoin.builder()
            .from(sb.hashJoin(leftKeys, rightKeys, HashJoin.JoinType.INNER, leftTable, rightTable))
            .residualExpression(
                sb.equal(
                    sb.fieldReference(Arrays.asList(leftTable, rightTable), 0),
                    sb.fieldReference(Arrays.asList(leftTable, rightTable), 5)))
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void mergeJoin() {
    List<Integer> leftKeys = Arrays.asList(0, 1);
    List<Integer> rightKeys = Arrays.asList(2, 0);
    Rel relWithoutKeys =
        MergeJoin.builder()
            .from(
                sb.mergeJoin(leftKeys, rightKeys, MergeJoin.JoinType.INNER, leftTable, rightTable))
            .build();
    verifyRoundTrip(relWithoutKeys);
  }

  @Test
  void mergeJoinWithResidualExpression() {
    List<Integer> leftKeys = Arrays.asList(0, 1);
    List<Integer> rightKeys = Arrays.asList(2, 0);
    // Residual filter over the combined left+right schema: left.a (I64, index 0) == right.f (I64,
    // index 5).
    Rel rel =
        MergeJoin.builder()
            .from(
                sb.mergeJoin(leftKeys, rightKeys, MergeJoin.JoinType.INNER, leftTable, rightTable))
            .residualExpression(
                sb.equal(
                    sb.fieldReference(Arrays.asList(leftTable, rightTable), 0),
                    sb.fieldReference(Arrays.asList(leftTable, rightTable), 5)))
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void nestedLoopJoin() {
    Rel rel =
        NestedLoopJoin.builder()
            .from(
                sb.nestedLoopJoin(
                    inputRels ->
                        sb.equal(sb.fieldReference(inputRels, 0), sb.fieldReference(inputRels, 5)),
                    NestedLoopJoin.JoinType.INNER,
                    leftTable,
                    rightTable))
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void lateralJoin() {
    // Condition over the combined left+right schema: left.a (I64, index 0) == right.f (I64,
    // index 5).
    Rel rel =
        LateralJoin.builder()
            .left(leftTable)
            .right(rightTable)
            .condition(
                sb.equal(
                    sb.fieldReference(Arrays.asList(leftTable, rightTable), 0),
                    sb.fieldReference(Arrays.asList(leftTable, rightTable), 5)))
            .joinType(Join.JoinType.INNER)
            .relAnchor(1)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void lateralJoinWithoutCondition() {
    // A lateral join with no join condition (the correlation lives inside the right input); the
    // unset expression must round-trip as an empty condition, not throw.
    Rel rel =
        LateralJoin.builder()
            .left(leftTable)
            .right(rightTable)
            .joinType(Join.JoinType.INNER)
            .relAnchor(1)
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void lateralJoinWithAnchorAndPostFilter() {
    // A lateral join sets a rel anchor so the right input can reference the current left row.
    Rel rel =
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
            .relAnchor(1)
            .build();
    verifyRoundTrip(rel);
  }
}
