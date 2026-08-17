package io.substrait.relation.physical;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Asserts the output record type of the physical join relations for every join type. The three
 * relations have the same output-schema semantics, so they share one expectation table and any
 * divergence between them fails here.
 */
class PhysicalJoinRecordTypeTest extends TestBase {

  final Rel leftTable = sb.namedScan(Arrays.asList("T1"), Arrays.asList("a"), Arrays.asList(R.I64));

  final Rel rightTable =
      sb.namedScan(Arrays.asList("T2"), Arrays.asList("b", "c"), Arrays.asList(R.I64, R.STRING));

  /**
   * The expected output schema per join type, for a single-column left input and a two-column right
   * input: an outer join pads the side that can go unmatched with nulls, and a semi or anti join
   * emits only the side it filters.
   */
  static Stream<Arguments> joinTypes() {
    return Stream.of(
        Arguments.of("UNKNOWN", R.struct(R.I64, R.I64, R.STRING)),
        Arguments.of("INNER", R.struct(R.I64, R.I64, R.STRING)),
        Arguments.of("OUTER", R.struct(N.I64, N.I64, N.STRING)),
        Arguments.of("LEFT", R.struct(R.I64, N.I64, N.STRING)),
        Arguments.of("RIGHT", R.struct(N.I64, R.I64, R.STRING)),
        Arguments.of("LEFT_SEMI", R.struct(R.I64)),
        Arguments.of("LEFT_ANTI", R.struct(R.I64)),
        Arguments.of("RIGHT_SEMI", R.struct(R.I64, R.STRING)),
        Arguments.of("RIGHT_ANTI", R.struct(R.I64, R.STRING)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("joinTypes")
  void hashJoinRecordType(String joinType, Type.Struct expected) {
    HashJoin join =
        sb.hashJoin(
            Collections.singletonList(0),
            Collections.singletonList(0),
            HashJoin.JoinType.valueOf(joinType),
            leftTable,
            rightTable);
    assertEquals(expected, join.getRecordType());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("joinTypes")
  void mergeJoinRecordType(String joinType, Type.Struct expected) {
    MergeJoin join =
        sb.mergeJoin(
            Collections.singletonList(0),
            Collections.singletonList(0),
            MergeJoin.JoinType.valueOf(joinType),
            leftTable,
            rightTable);
    assertEquals(expected, join.getRecordType());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("joinTypes")
  void nestedLoopJoinRecordType(String joinType, Type.Struct expected) {
    NestedLoopJoin join =
        sb.nestedLoopJoin(
            inputRels -> sb.equal(sb.fieldReference(inputRels, 0), sb.fieldReference(inputRels, 1)),
            NestedLoopJoin.JoinType.valueOf(joinType),
            leftTable,
            rightTable);
    assertEquals(expected, join.getRecordType());
  }

  /** A join type added to any of the three enums must be added to the expectation table too. */
  @Test
  void expectationTableCoversEveryJoinType() {
    Set<String> covered =
        joinTypes().map(arguments -> (String) arguments.get()[0]).collect(Collectors.toSet());
    assertEquals(names(HashJoin.JoinType.values()), covered);
    assertEquals(names(MergeJoin.JoinType.values()), covered);
    assertEquals(names(NestedLoopJoin.JoinType.values()), covered);
  }

  private static Set<String> names(Enum<?>[] values) {
    return Arrays.stream(values).map(Enum::name).collect(Collectors.toSet());
  }
}
