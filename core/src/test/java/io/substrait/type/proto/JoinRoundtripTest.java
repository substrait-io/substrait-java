package io.substrait.type.proto;

<<<<<<< HEAD
=======
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

>>>>>>> 408672a (fix: improve tests)
import io.substrait.TestBase;
import io.substrait.relation.Rel;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.NestedLoopJoin;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class JoinRoundtripTest extends TestBase {

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
  void nestedLoopJoin() {
    Rel relWithCustomExpression =
        NestedLoopJoin.builder()
            .from(
                b.nestedLoopJoin(
                    __ -> b.bool(false), NestedLoopJoin.JoinType.INNER, leftTable, rightTable))
            .build();
    verifyRoundTrip(relWithCustomExpression);

    Rel relWithDefaultExpression =
        NestedLoopJoin.builder()
            .from(
                b.nestedLoopJoin(
                    __ -> b.bool(true), NestedLoopJoin.JoinType.INNER, leftTable, rightTable))
            .build();
    verifyRoundTrip(relWithDefaultExpression);

    Rel relWithoutExpression =
        NestedLoopJoin.builder()
            .from(b.nestedLoopJoin(NestedLoopJoin.JoinType.INNER, leftTable, rightTable))
            .build();
    assertNotEquals(relWithDefaultExpression, relWithoutExpression);
    assertEquals(relWithDefaultExpression, roundTrip(relWithoutExpression));
  }
}
