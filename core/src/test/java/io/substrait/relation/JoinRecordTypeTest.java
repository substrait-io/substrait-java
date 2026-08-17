package io.substrait.relation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.substrait.TestBase;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.NestedLoopJoin;
import io.substrait.type.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Asserts the output record type of every join relation for every join type it accepts. The
 * logical, lateral and physical joins share one expectation table because they share the same
 * output-schema semantics, so a divergence between them fails here.
 *
 * <p>Each relation is parameterized over its own join-type enum, so a join type added to any of
 * them fails until an expectation is declared for it.
 */
class JoinRecordTypeTest extends TestBase {

  /** The expected output schema of each join type, see {@link #expectedRecordTypes()}. */
  private static final Map<String, Type.Struct> EXPECTED_RECORD_TYPES = expectedRecordTypes();

  final Rel leftTable = sb.namedScan(Arrays.asList("T1"), Arrays.asList("a"), Arrays.asList(R.I64));

  final Rel rightTable =
      sb.namedScan(Arrays.asList("T2"), Arrays.asList("b", "c"), Arrays.asList(R.I64, R.STRING));

  /**
   * The expected output schema per join type, for the single-column {@link #leftTable} and the
   * two-column {@link #rightTable}: an outer join makes the side that can go unmatched nullable,
   * and a semi or anti join emits only the side it filters.
   */
  private static Map<String, Type.Struct> expectedRecordTypes() {
    Map<String, Type.Struct> expected = new LinkedHashMap<>();
    expected.put("UNKNOWN", R.struct(R.I64, R.I64, R.STRING));
    expected.put("INNER", R.struct(R.I64, R.I64, R.STRING));
    expected.put("OUTER", R.struct(N.I64, N.I64, N.STRING));
    expected.put("LEFT", R.struct(R.I64, N.I64, N.STRING));
    expected.put("RIGHT", R.struct(N.I64, R.I64, R.STRING));
    expected.put("LEFT_SEMI", R.struct(R.I64));
    expected.put("LEFT_ANTI", R.struct(R.I64));
    expected.put("RIGHT_SEMI", R.struct(R.I64, R.STRING));
    expected.put("RIGHT_ANTI", R.struct(R.I64, R.STRING));
    // A single join emits at most one partner row per row of the side it preserves, so it pads the
    // other side like the outer join of the same orientation.
    expected.put("LEFT_SINGLE", R.struct(R.I64, N.I64, N.STRING));
    expected.put("RIGHT_SINGLE", R.struct(N.I64, R.I64, R.STRING));
    // A mark join emits only the side it preserves, plus a boolean "mark" column. The mark is
    // nullable because the match state is 3-valued: true (a partner matched), false (no partner and
    // no NULL comparisons) or NULL (no partner but some comparison was NULL).
    expected.put("LEFT_MARK", R.struct(R.I64, N.BOOLEAN));
    expected.put("RIGHT_MARK", R.struct(R.I64, R.STRING, N.BOOLEAN));
    return expected;
  }

  @ParameterizedTest
  @EnumSource(Join.JoinType.class)
  void joinRecordType(Join.JoinType joinType) {
    // The join condition does not affect the output schema, so it is left unset here.
    Join join = Join.builder().left(leftTable).right(rightTable).joinType(joinType).build();
    assertEquals(expected(joinType), join.getRecordType());
  }

  @ParameterizedTest
  @EnumSource(
      value = Join.JoinType.class,
      names = {
        "UNKNOWN",
        "OUTER",
        "RIGHT",
        "RIGHT_SEMI",
        "RIGHT_ANTI",
        "RIGHT_SINGLE",
        "RIGHT_MARK"
      },
      // A lateral join rejects these; LateralJoinTest covers that rejection.
      mode = EnumSource.Mode.EXCLUDE)
  void lateralJoinRecordType(Join.JoinType joinType) {
    LateralJoin join =
        LateralJoin.builder()
            .left(leftTable)
            .right(rightTable)
            .joinType(joinType)
            .relAnchor(1)
            .build();
    assertEquals(expected(joinType), join.getRecordType());
  }

  @ParameterizedTest
  @EnumSource(HashJoin.JoinType.class)
  void hashJoinRecordType(HashJoin.JoinType joinType) {
    HashJoin join =
        sb.hashJoin(
            Collections.singletonList(0),
            Collections.singletonList(0),
            joinType,
            leftTable,
            rightTable);
    assertEquals(expected(joinType), join.getRecordType());
  }

  @ParameterizedTest
  @EnumSource(MergeJoin.JoinType.class)
  void mergeJoinRecordType(MergeJoin.JoinType joinType) {
    MergeJoin join =
        sb.mergeJoin(
            Collections.singletonList(0),
            Collections.singletonList(0),
            joinType,
            leftTable,
            rightTable);
    assertEquals(expected(joinType), join.getRecordType());
  }

  @ParameterizedTest
  @EnumSource(NestedLoopJoin.JoinType.class)
  void nestedLoopJoinRecordType(NestedLoopJoin.JoinType joinType) {
    NestedLoopJoin join =
        sb.nestedLoopJoin(
            inputs -> sb.equal(sb.fieldReference(inputs, 0), sb.fieldReference(inputs, 1)),
            joinType,
            leftTable,
            rightTable);
    assertEquals(expected(joinType), join.getRecordType());
  }

  private static Type.Struct expected(Enum<?> joinType) {
    Type.Struct expected = EXPECTED_RECORD_TYPES.get(joinType.name());
    assertNotNull(expected, "no expected record type declared for join type " + joinType.name());
    return expected;
  }
}
