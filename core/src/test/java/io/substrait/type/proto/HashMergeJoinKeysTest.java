package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.relation.Rel;
import io.substrait.relation.physical.ComparisonJoinKey;
import io.substrait.relation.physical.ComparisonJoinKey.SimpleComparisonType;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link HashJoin}/{@link MergeJoin} round-trip their {@code keys} field, including
 * non-equality and custom comparisons, and that the deprecated {@link HashJoin#getLeftKeys()}
 * convenience views derive from {@code keys}.
 */
class HashMergeJoinKeysTest extends TestBase {

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

  final HashJoin hashJoin =
      sb.hashJoin(
          Arrays.asList(0, 1), Arrays.asList(2, 0), HashJoin.JoinType.INNER, leftTable, rightTable);

  final MergeJoin mergeJoin =
      sb.mergeJoin(
          Arrays.asList(0, 1),
          Arrays.asList(2, 0),
          MergeJoin.JoinType.INNER,
          leftTable,
          rightTable);

  @Test
  void hashJoinRoundTrip() {
    verifyRoundTrip(hashJoin);
  }

  @Test
  void mergeJoinRoundTrip() {
    verifyRoundTrip(mergeJoin);
  }

  // The DSL builds equality keys; the deprecated convenience views should derive from them.
  @Test
  void deprecatedViewsDeriveFromKeys() {
    assertEquals(
        hashJoin.getKeys().stream().map(ComparisonJoinKey::getLeft).collect(Collectors.toList()),
        hashJoin.getLeftKeys());
    assertEquals(
        hashJoin.getKeys().stream().map(ComparisonJoinKey::getRight).collect(Collectors.toList()),
        hashJoin.getRightKeys());
  }

  // Non-EQ simple comparisons and custom comparison functions survive a round trip.
  @Test
  void fullFidelityRoundTrip() {
    List<ComparisonJoinKey> keys =
        Arrays.asList(
            ComparisonJoinKey.of(
                sb.fieldReference(leftTable, 0),
                sb.fieldReference(rightTable, 2),
                SimpleComparisonType.EQ),
            ComparisonJoinKey.of(
                sb.fieldReference(leftTable, 1),
                sb.fieldReference(rightTable, 0),
                SimpleComparisonType.IS_NOT_DISTINCT_FROM),
            ComparisonJoinKey.builder()
                .left(sb.fieldReference(leftTable, 2))
                .right(sb.fieldReference(rightTable, 1))
                .comparison(ComparisonJoinKey.CustomComparison.of(42))
                .build());

    Rel hash =
        HashJoin.builder()
            .left(leftTable)
            .right(rightTable)
            .keys(keys)
            .joinType(HashJoin.JoinType.INNER)
            .build();
    verifyRoundTrip(hash);

    Rel merge =
        MergeJoin.builder()
            .left(leftTable)
            .right(rightTable)
            .keys(keys)
            .joinType(MergeJoin.JoinType.INNER)
            .build();
    verifyRoundTrip(merge);
  }
}
