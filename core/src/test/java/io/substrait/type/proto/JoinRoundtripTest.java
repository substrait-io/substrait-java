package io.substrait.type.proto;

import io.substrait.TestBase;
import io.substrait.relation.Rel;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.NestedLoopJoin;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class JoinRoundtripTest extends TestBase {

  final Rel leftTable =
      b.namedScan(
          Arrays.asList("T1"),
          Arrays.asList("a", "b", "c"),
          Arrays.asList(R.I64, R.FP64, R.STRING));

  final Rel rightTable =
      b.namedScan(
          Arrays.asList("T2"),
          Arrays.asList("d", "e", "f"),
          Arrays.asList(R.FP64, R.STRING, R.I64));

  @Test
  void hashJoin() {
    List<Integer> leftKeys = Arrays.asList(0, 1);
    List<Integer> rightKeys = Arrays.asList(2, 0);
    Rel relWithoutKeys =
        HashJoin.builder()
            .from(b.hashJoin(leftKeys, rightKeys, HashJoin.JoinType.INNER, leftTable, rightTable))
            .build();
    verifyRoundTrip(relWithoutKeys);
  }

  @Test
  void mergeJoin() {
    List<Integer> leftKeys = Arrays.asList(0, 1);
    List<Integer> rightKeys = Arrays.asList(2, 0);
    Rel relWithoutKeys =
        MergeJoin.builder()
            .from(b.mergeJoin(leftKeys, rightKeys, MergeJoin.JoinType.INNER, leftTable, rightTable))
            .build();
    verifyRoundTrip(relWithoutKeys);
  }

  @Test
  void nestedLoopJoin() {
    List<Rel> inputRels = Arrays.asList(leftTable, rightTable);
    Rel rel =
        NestedLoopJoin.builder()
            .from(
                b.nestedLoopJoin(
                    __ -> b.equal(b.fieldReference(inputRels, 0), b.fieldReference(inputRels, 5)),
                    NestedLoopJoin.JoinType.INNER,
                    leftTable,
                    rightTable))
            .build();
    verifyRoundTrip(rel);
  }
}
