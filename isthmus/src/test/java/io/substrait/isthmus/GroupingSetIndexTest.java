package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.expression.ExpressionCreator;
import io.substrait.relation.Rel;
import io.substrait.type.NamedStruct;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelRunners;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class GroupingSetIndexTest extends PlanTestBase {

  @Test
  void indexFollowsDeclaredSetOrder() throws SQLException {
    Rel aggregate = aggregate(List.of(List.of(1), List.of(0), List.of()), input());
    assertRowsAndRoundTrip(
        aggregate,
        List.of(
            row(10, null, 2L, 0),
            row(null, 1, 1L, 1),
            row(null, 2, 1L, 1),
            row(null, null, 2L, 2)));
  }

  @Test
  void nestedGroupingSetsWithoutMeasuresKeepTheirIndices() throws SQLException {
    Rel aggregate =
        sb.aggregate(
            rel -> List.of(sb.grouping(rel, 0), sb.grouping(rel, 0, 1)),
            rel -> List.of(),
            Optional.empty(),
            input());
    assertRowsAndRoundTrip(
        aggregate, List.of(row(1, null, 0), row(2, null, 0), row(1, 10, 1), row(2, 10, 1)));
  }

  @Test
  void computedGroupingKeysKeepTheirIndices() throws SQLException {
    Rel aggregate =
        sb.aggregate(
            rel ->
                List.of(
                    sb.grouping(sb.add(sb.fieldReference(rel, 0), sb.i32(1))),
                    sb.grouping(rel, 1),
                    sb.grouping(sb.add(sb.fieldReference(rel, 0), sb.i32(1)))),
            rel -> List.of(sb.countStar()),
            Optional.empty(),
            input());
    assertRowsAndRoundTrip(
        aggregate,
        List.of(
            row(2, null, 1L, 0),
            row(3, null, 1L, 0),
            row(null, 10, 2L, 1),
            row(2, null, 1L, 2),
            row(3, null, 1L, 2)));
  }

  @Test
  void duplicateSetsKeepTheirOwnIndices() throws SQLException {
    Rel aggregate =
        aggregate(List.of(List.of(1), List.of(0), List.of(1), List.of(), List.of()), input());
    assertRowsAndRoundTrip(
        aggregate,
        List.of(
            row(10, null, 2L, 0),
            row(null, 1, 1L, 1),
            row(null, 2, 1L, 1),
            row(10, null, 2L, 2),
            row(null, null, 2L, 3),
            row(null, null, 2L, 4)));
  }

  @Test
  void nullKeysDoNotIdentifyGroupingSets() throws SQLException {
    Rel input =
        virtualTable(
            NamedStruct.of(List.of("a", "b"), R.struct(N.I32, N.I32)),
            List.of(ExpressionCreator.i32(true, 1), ExpressionCreator.typedNull(N.I32)),
            List.of(ExpressionCreator.typedNull(N.I32), ExpressionCreator.typedNull(N.I32)));
    assertRowsAndRoundTrip(
        aggregate(List.of(List.of(1), List.of(0), List.of()), input),
        List.of(
            row(null, null, 2L, 0),
            row(null, 1, 1L, 1),
            row(null, null, 1L, 1),
            row(null, null, 2L, 2)));
  }

  @Test
  void emptyInputStillProducesEachEmptySet() throws SQLException {
    Rel input = virtualTable(NamedStruct.of(List.of("a", "b"), R.struct(R.I32, R.I32)));
    assertRowsAndRoundTrip(
        aggregate(List.of(List.of(), List.of()), input), List.of(row(0L, 0), row(0L, 1)));
  }

  @Test
  @Disabled("Calcite 1.42.0 drops an empty group on empty input with mixed grouping sets")
  void mixedGroupingSetsOnEmptyInputKeepEveryEmptySet() throws SQLException {
    Rel input = virtualTable(NamedStruct.of(List.of("a", "b"), R.struct(R.I32, R.I32)));
    assertRowsAndRoundTrip(
        aggregate(List.of(List.of(), List.of(0), List.of()), input),
        List.of(row(null, 0L, 0), row(null, 0L, 2)));
  }

  @Test
  void emitCanReorderAndRepeatTheIndex() throws SQLException {
    Rel aggregate = aggregate(List.of(List.of(1), List.of(0)), input());
    assertRowsAndRoundTrip(
        aggregate.withRemap(Optional.of(Rel.Remap.of(List.of(3, 2, 3)))),
        List.of(row(0, 2L, 0), row(1, 1L, 1), row(1, 1L, 1)));
  }

  @Test
  void aFilterSelectsTheDeclaredOccurrenceOfASet() throws SQLException {
    Rel aggregate = aggregate(List.of(List.of(1), List.of(0), List.of(1)), input());
    assertRowsAndRoundTrip(
        sb.filter(rel -> sb.equal(sb.fieldReference(rel, 3), sb.i32(2)), aggregate),
        List.of(row(10, null, 2L, 2)));
  }

  @Test
  void omittingTheIndexKeepsDuplicateRows() throws SQLException {
    Rel aggregate = aggregate(List.of(List.of(1), List.of(0), List.of(1)), input());
    assertRowsAndRoundTrip(
        aggregate.withRemap(Optional.of(Rel.Remap.of(List.of(0, 1, 2)))),
        List.of(row(10, null, 2L), row(10, null, 2L), row(null, 1, 1L), row(null, 2, 1L)));
  }

  @Test
  void indexIsNotLimitedByTheWidthOfAGroupingMask() throws SQLException {
    int width = 65;
    Rel input =
        virtualTable(
            NamedStruct.of(
                IntStream.range(0, width).mapToObj(i -> "c" + i).collect(Collectors.toList()),
                R.struct(
                    IntStream.range(0, width).mapToObj(i -> R.I32).collect(Collectors.toList()))),
            IntStream.range(0, width).mapToObj(sb::i32).collect(Collectors.toList()));
    Rel aggregate =
        aggregate(
            List.of(IntStream.range(0, width).boxed().collect(Collectors.toList()), List.of()),
            input);
    assertRowsAndRoundTrip(
        aggregate.withRemap(Optional.of(Rel.Remap.of(List.of(width + 1)))),
        List.of(row(0), row(1)));
  }

  @Test
  void groupingMasksPreserveArgumentOrderAndMeasurePositions() throws SQLException {
    builder.push(substraitToCalcite.convert(input()));
    RelNode calcite =
        builder
            .aggregate(
                builder.groupKey(
                    ImmutableBitSet.of(0, 1),
                    List.of(ImmutableBitSet.of(0), ImmutableBitSet.of(1), ImmutableBitSet.of())),
                builder.aggregateCall(
                    SqlStdOperatorTable.GROUPING, builder.field(1), builder.field(0)),
                builder.countStar("n"),
                builder.aggregateCall(
                    SqlStdOperatorTable.GROUPING_ID, builder.field(0), builder.field(1)))
            .build();
    Rel exported =
        SubstraitRelVisitor.convert(RelRoot.of(calcite, SqlKind.SELECT), converterProvider)
            .getInput();
    assertRowsAndRoundTrip(
        exported,
        List.of(
            row(1, null, 2L, 1L, 1L),
            row(2, null, 2L, 1L, 1L),
            row(null, 10, 1L, 2L, 2L),
            row(null, null, 3L, 2L, 3L)));
  }

  private Rel input() {
    return virtualTable(
        NamedStruct.of(List.of("a", "b"), R.struct(R.I32, R.I32)),
        List.of(sb.i32(1), sb.i32(10)),
        List.of(sb.i32(2), sb.i32(10)));
  }

  private Rel aggregate(List<List<Integer>> sets, Rel input) {
    return sb.aggregate(
        rel ->
            sets.stream()
                .map(set -> sb.grouping(rel, set.stream().mapToInt(Integer::intValue).toArray()))
                .collect(Collectors.toList()),
        rel -> List.of(sb.countStar()),
        Optional.empty(),
        input);
  }

  private void assertRowsAndRoundTrip(Rel rel, List<List<Object>> expected) throws SQLException {
    RelNode calcite = substraitToCalcite.convert(rel);
    assertEquals(
        multiset(expected), multiset(execute(calcite)), () -> RelOptUtil.toString(calcite));
    assertRowMatch(calcite.getRowType(), rel.getRecordType().fields());
    Rel exported =
        SubstraitRelVisitor.convert(RelRoot.of(calcite, SqlKind.SELECT), converterProvider)
            .getInput();
    assertEquals(rel.getRecordType(), exported.getRecordType());
    assertEquals(multiset(expected), multiset(execute(substraitToCalcite.convert(exported))));
  }

  private static List<Object> row(Object... values) {
    return Arrays.asList(values);
  }

  private static Map<List<Object>, Long> multiset(List<List<Object>> rows) {
    return rows.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
  }

  private static List<List<Object>> execute(RelNode rel) throws SQLException {
    try (PreparedStatement statement = RelRunners.run(rel);
        ResultSet result = statement.executeQuery()) {
      List<List<Object>> rows = new ArrayList<>();
      while (result.next()) {
        List<Object> row = new ArrayList<>();
        for (int column = 1; column <= result.getMetaData().getColumnCount(); column++) {
          row.add(result.getObject(column));
        }
        rows.add(row);
      }
      return rows;
    }
  }
}
