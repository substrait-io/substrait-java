package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.proto.HashJoinRel;
import io.substrait.proto.MergeJoinRel;
import io.substrait.proto.NestedLoopJoinRel;
import io.substrait.relation.Join;
import io.substrait.relation.LateralJoin;
import io.substrait.relation.Rel;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.NestedLoopJoin;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

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

  @ParameterizedTest
  @EnumSource(value = Join.JoinType.class, names = "UNKNOWN", mode = EnumSource.Mode.EXCLUDE)
  void postJoinFilterUsesOutputSchemaBeforeEmit(Join.JoinType joinType) {
    Join join =
        sb.join(
            input -> sb.equal(sb.fieldReference(input, 0), sb.fieldReference(input, 5)),
            joinType,
            leftTable,
            rightTable);
    Join filtered =
        Join.builder()
            .from(join)
            .postJoinFilter(
                sb.and(
                    sb.isNull(sb.fieldReference(join, 0)),
                    sb.isNull(sb.fieldReference(join, join.getRecordType().fields().size() - 1))))
            .remap(sb.remap(0))
            .build();

    verifyRoundTrip(filtered);
  }

  @ParameterizedTest
  @EnumSource(
      value = HashJoinRel.JoinType.class,
      names = {"JOIN_TYPE_UNSPECIFIED", "UNRECOGNIZED"},
      mode = EnumSource.Mode.EXCLUDE)
  void hashJoin(HashJoinRel.JoinType joinType) {
    List<Integer> leftKeys = Arrays.asList(0, 1);
    List<Integer> rightKeys = Arrays.asList(2, 0);
    Rel relWithoutKeys =
        HashJoin.builder()
            .from(
                sb.hashJoin(
                    leftKeys,
                    rightKeys,
                    HashJoin.JoinType.fromProto(joinType),
                    leftTable,
                    rightTable))
            .build();
    assertEquals(joinType, relProtoConverter.toProto(relWithoutKeys).getHashJoin().getType());
    verifyRoundTrip(relWithoutKeys);
  }

  @ParameterizedTest
  @EnumSource(value = HashJoin.JoinType.class, names = "UNKNOWN", mode = EnumSource.Mode.EXCLUDE)
  void hashPostJoinFilterUsesOutputSchemaBeforeEmit(HashJoin.JoinType joinType) {
    HashJoin join = sb.hashJoin(List.of(0), List.of(2), joinType, leftTable, rightTable);
    verifyRoundTrip(
        HashJoin.builder()
            .from(join)
            .postJoinFilter(
                sb.and(
                    sb.isNull(sb.fieldReference(join, 0)),
                    sb.isNull(sb.fieldReference(join, join.getRecordType().fields().size() - 1))))
            .remap(sb.remap(0))
            .build());
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

  @ParameterizedTest
  @EnumSource(
      value = MergeJoinRel.JoinType.class,
      names = {"JOIN_TYPE_UNSPECIFIED", "UNRECOGNIZED"},
      mode = EnumSource.Mode.EXCLUDE)
  void mergeJoin(MergeJoinRel.JoinType joinType) {
    List<Integer> leftKeys = Arrays.asList(0, 1);
    List<Integer> rightKeys = Arrays.asList(2, 0);
    Rel relWithoutKeys =
        MergeJoin.builder()
            .from(
                sb.mergeJoin(
                    leftKeys,
                    rightKeys,
                    MergeJoin.JoinType.fromProto(joinType),
                    leftTable,
                    rightTable))
            .build();
    assertEquals(joinType, relProtoConverter.toProto(relWithoutKeys).getMergeJoin().getType());
    verifyRoundTrip(relWithoutKeys);
  }

  @ParameterizedTest
  @EnumSource(value = MergeJoin.JoinType.class, names = "UNKNOWN", mode = EnumSource.Mode.EXCLUDE)
  void mergePostJoinFilterUsesOutputSchemaBeforeEmit(MergeJoin.JoinType joinType) {
    MergeJoin join = sb.mergeJoin(List.of(0), List.of(2), joinType, leftTable, rightTable);
    verifyRoundTrip(
        MergeJoin.builder()
            .from(join)
            .postJoinFilter(
                sb.and(
                    sb.isNull(sb.fieldReference(join, 0)),
                    sb.isNull(sb.fieldReference(join, join.getRecordType().fields().size() - 1))))
            .remap(sb.remap(0))
            .build());
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

  @ParameterizedTest
  @EnumSource(
      value = NestedLoopJoinRel.JoinType.class,
      names = {"JOIN_TYPE_UNSPECIFIED", "UNRECOGNIZED"},
      mode = EnumSource.Mode.EXCLUDE)
  void nestedLoopJoin(NestedLoopJoinRel.JoinType joinType) {
    Rel rel =
        NestedLoopJoin.builder()
            .from(
                sb.nestedLoopJoin(
                    inputRels ->
                        sb.equal(sb.fieldReference(inputRels, 0), sb.fieldReference(inputRels, 5)),
                    NestedLoopJoin.JoinType.fromProto(joinType),
                    leftTable,
                    rightTable))
            .build();
    assertEquals(joinType, relProtoConverter.toProto(rel).getNestedLoopJoin().getType());
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

  @ParameterizedTest
  @EnumSource(
      value = Join.JoinType.class,
      names = {"INNER", "LEFT", "LEFT_SEMI", "LEFT_ANTI", "LEFT_SINGLE", "LEFT_MARK"})
  void lateralJoinWithAnchorAndPostFilter(Join.JoinType joinType) {
    // A lateral join sets a rel anchor so the right input can reference the current left row.
    LateralJoin join =
        LateralJoin.builder()
            .left(leftTable)
            .right(rightTable)
            .condition(
                sb.equal(
                    sb.fieldReference(Arrays.asList(leftTable, rightTable), 0),
                    sb.fieldReference(Arrays.asList(leftTable, rightTable), 5)))
            .joinType(joinType)
            .relAnchor(1)
            .build();
    verifyRoundTrip(
        LateralJoin.builder()
            .from(join)
            .postJoinFilter(
                sb.and(
                    sb.isNull(sb.fieldReference(join, 0)),
                    sb.isNull(sb.fieldReference(join, join.getRecordType().fields().size() - 1))))
            .remap(sb.remap(0))
            .build());
  }
}
