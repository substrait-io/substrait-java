package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
 * Verifies that {@link HashJoin}/{@link MergeJoin} consume both the deprecated {@code
 * left_keys}/{@code right_keys} proto fields and the new {@code keys} field, always produce the new
 * {@code keys} field, and additionally produce the deprecated fields when (and only when) every key
 * is a plain {@code EQ} comparison they can represent without loss.
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

  // A plain EQ join must populate the new keys field and, for backwards compatibility with
  // consumers that have not yet adopted it, the deprecated left_keys/right_keys as well.
  @Test
  void producesBothKeysForEqJoins() {
    io.substrait.proto.HashJoinRel proto = relProtoConverter.toProto(hashJoin).getHashJoin();
    assertEquals(2, proto.getKeysCount());
    assertEquals(
        proto.getKeysList().stream()
            .map(io.substrait.proto.ComparisonJoinKey::getLeft)
            .collect(Collectors.toList()),
        proto.getLeftKeysList());
    assertEquals(
        proto.getKeysList().stream()
            .map(io.substrait.proto.ComparisonJoinKey::getRight)
            .collect(Collectors.toList()),
        proto.getRightKeysList());

    io.substrait.proto.MergeJoinRel mergeProto =
        relProtoConverter.toProto(mergeJoin).getMergeJoin();
    assertEquals(2, mergeProto.getKeysCount());
    assertEquals(
        mergeProto.getKeysList().stream()
            .map(io.substrait.proto.ComparisonJoinKey::getLeft)
            .collect(Collectors.toList()),
        mergeProto.getLeftKeysList());
    assertEquals(
        mergeProto.getKeysList().stream()
            .map(io.substrait.proto.ComparisonJoinKey::getRight)
            .collect(Collectors.toList()),
        mergeProto.getRightKeysList());
  }

  // The deprecated fields cannot represent IS_NOT_DISTINCT_FROM, MIGHT_EQUAL or custom comparisons,
  // so a join containing any such key must only populate the new keys field. Otherwise an old
  // consumer would silently misinterpret those keys as plain equality.
  @Test
  void producesOnlyNewKeysForLossyComparisons() {
    List<ComparisonJoinKey> keys =
        Arrays.asList(
            ComparisonJoinKey.of(
                sb.fieldReference(leftTable, 0),
                sb.fieldReference(rightTable, 2),
                SimpleComparisonType.EQ),
            ComparisonJoinKey.of(
                sb.fieldReference(leftTable, 1),
                sb.fieldReference(rightTable, 0),
                SimpleComparisonType.IS_NOT_DISTINCT_FROM));

    HashJoin hash =
        HashJoin.builder()
            .left(leftTable)
            .right(rightTable)
            .keys(keys)
            .joinType(HashJoin.JoinType.INNER)
            .build();
    io.substrait.proto.HashJoinRel proto = relProtoConverter.toProto(hash).getHashJoin();
    assertEquals(2, proto.getKeysCount());
    assertTrue(proto.getLeftKeysList().isEmpty());
    assertTrue(proto.getRightKeysList().isEmpty());

    MergeJoin merge =
        MergeJoin.builder()
            .left(leftTable)
            .right(rightTable)
            .keys(keys)
            .joinType(MergeJoin.JoinType.INNER)
            .build();
    io.substrait.proto.MergeJoinRel mergeProto = relProtoConverter.toProto(merge).getMergeJoin();
    assertEquals(2, mergeProto.getKeysCount());
    assertTrue(mergeProto.getLeftKeysList().isEmpty());
    assertTrue(mergeProto.getRightKeysList().isEmpty());
  }

  // A plan from a legacy producer (only deprecated fields set) is consumed and mapped to EQ keys.
  @Test
  void consumesLegacyHashJoin() {
    io.substrait.proto.HashJoinRel modern = relProtoConverter.toProto(hashJoin).getHashJoin();
    io.substrait.proto.HashJoinRel legacy =
        modern.toBuilder()
            .clearKeys()
            .clearLeftKeys()
            .clearRightKeys()
            .addAllLeftKeys(
                modern.getKeysList().stream()
                    .map(io.substrait.proto.ComparisonJoinKey::getLeft)
                    .collect(Collectors.toList()))
            .addAllRightKeys(
                modern.getKeysList().stream()
                    .map(io.substrait.proto.ComparisonJoinKey::getRight)
                    .collect(Collectors.toList()))
            .build();

    Rel result =
        protoRelConverter.from(io.substrait.proto.Rel.newBuilder().setHashJoin(legacy).build());
    assertEquals(hashJoin, result);
    assertEquals(2, ((HashJoin) result).getKeys().size());
    ((HashJoin) result)
        .getKeys()
        .forEach(
            k ->
                assertEquals(
                    ComparisonJoinKey.SimpleComparison.of(SimpleComparisonType.EQ),
                    k.getComparison()));
  }

  @Test
  void consumesLegacyMergeJoin() {
    io.substrait.proto.MergeJoinRel modern = relProtoConverter.toProto(mergeJoin).getMergeJoin();
    io.substrait.proto.MergeJoinRel legacy =
        modern.toBuilder()
            .clearKeys()
            .clearLeftKeys()
            .clearRightKeys()
            .addAllLeftKeys(
                modern.getKeysList().stream()
                    .map(io.substrait.proto.ComparisonJoinKey::getLeft)
                    .collect(Collectors.toList()))
            .addAllRightKeys(
                modern.getKeysList().stream()
                    .map(io.substrait.proto.ComparisonJoinKey::getRight)
                    .collect(Collectors.toList()))
            .build();

    Rel result =
        protoRelConverter.from(io.substrait.proto.Rel.newBuilder().setMergeJoin(legacy).build());
    assertEquals(mergeJoin, result);
  }

  // When both the deprecated fields and the new keys are present, keys wins.
  @Test
  void prefersNewKeysOverDeprecated() {
    io.substrait.proto.HashJoinRel modern = relProtoConverter.toProto(hashJoin).getHashJoin();
    // Add bogus deprecated keys that point at different fields than the real keys.
    io.substrait.proto.HashJoinRel both =
        modern.toBuilder()
            .addLeftKeys(modern.getKeys(0).getRight())
            .addRightKeys(modern.getKeys(0).getLeft())
            .build();

    Rel result =
        protoRelConverter.from(io.substrait.proto.Rel.newBuilder().setHashJoin(both).build());
    assertEquals(hashJoin, result);
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
