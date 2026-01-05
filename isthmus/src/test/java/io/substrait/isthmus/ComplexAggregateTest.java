package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.ImmutableAggregateFunctionInvocation;
import io.substrait.relation.Aggregate;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.Test;

class ComplexAggregateTest extends PlanTestBase {

  private List<Type> columnTypes = List.of(R.I32, R.I32, R.I32, R.I32);
  private List<String> columnNames = List.of("a", "b", "c", "d");
  private NamedScan table = sb.namedScan(List.of("example"), columnNames, columnTypes);

  private Aggregate.Grouping emptyGrouping = Aggregate.Grouping.builder().build();

  Aggregate.Measure withPreMeasureFilter(Aggregate.Measure measure, Expression preMeasureFilter) {
    return Aggregate.Measure.builder().from(measure).preMeasureFilter(preMeasureFilter).build();
  }

  Aggregate.Measure withSort(Aggregate.Measure measure, List<Expression.SortField> sortFields) {
    ImmutableAggregateFunctionInvocation afi =
        AggregateFunctionInvocation.builder().from(measure.getFunction()).sort(sortFields).build();
    return Aggregate.Measure.builder().from(measure).function(afi).build();
  }

  /**
   * Check that:
   *
   * <ol>
   *   <li>The {@code pojo} pojo given is transformed as expected by {@link
   *       PreCalciteAggregateValidator.PreCalciteAggregateTransformer#transformToValidCalciteAggregate}
   *   <li>The {@code} (original) pojo can be converted to Calcite without issues
   * </ol>
   *
   * @param pojo a pojo that requires transformation for use in Calcite
   * @param expectedTransform the expected transformation output
   */
  protected void validateAggregateTransformation(Aggregate pojo, Rel expectedTransform) {
    Aggregate converterPojo =
        PreCalciteAggregateValidator.PreCalciteAggregateTransformer
            .transformToValidCalciteAggregate(pojo);
    assertEquals(expectedTransform, converterPojo);

    // Substrait POJO -> Calcite
    substraitToCalcite.convert(pojo);
  }

  @Test
  void handleComplexMeasureArgument() {
    // SELECT sum(c + 7) FROM example
    Aggregate rel =
        sb.aggregate(
            input -> emptyGrouping,
            input -> List.of(sb.sum(sb.add(sb.fieldReference(input, 2), sb.i32(7)))),
            table);

    Aggregate expectedFinal =
        sb.aggregate(
            input -> emptyGrouping,
            // sum call references input field
            input -> List.of(sb.sum(input, 4)),
            sb.project(
                // add call is moved to child project
                input -> List.of(sb.add(sb.fieldReference(input, 2), sb.i32(7))),
                table));

    validateAggregateTransformation(rel, expectedFinal);
  }

  @Test
  void handleComplexPreMeasureFilter() {
    // SELECT sum(a) FILTER (b = 42) FROM example
    Aggregate rel =
        sb.aggregate(
            input -> emptyGrouping,
            input ->
                List.of(
                    withPreMeasureFilter(
                        sb.sum(input, 0), sb.equal(sb.fieldReference(input, 1), sb.i32(42)))),
            table);

    Aggregate expectedFinal =
        sb.aggregate(
            input -> emptyGrouping,
            input -> List.of(withPreMeasureFilter(sb.sum(input, 0), sb.fieldReference(input, 4))),
            sb.project(input -> List.of(sb.equal(sb.fieldReference(input, 1), sb.i32(42))), table));

    validateAggregateTransformation(rel, expectedFinal);
  }

  @Test
  void handleComplexSortingArguments() {
    // SELECT sum(d ORDER BY -b ASC) FROM example
    Aggregate rel =
        sb.aggregate(
            input -> emptyGrouping,
            input ->
                List.of(
                    withSort(
                        sb.sum(input, 3),
                        List.of(
                            sb.sortField(
                                sb.negate(sb.fieldReference(input, 1)),
                                Expression.SortDirection.ASC_NULLS_FIRST)))),
            table);

    Aggregate expectedFinal =
        sb.aggregate(
            input -> emptyGrouping,
            input ->
                List.of(
                    withSort(
                        sb.sum(input, 3),
                        List.of(
                            sb.sortField(
                                sb.fieldReference(input, 4),
                                Expression.SortDirection.ASC_NULLS_FIRST)))),
            sb.project(
                // negate call is moved to child project
                input -> List.of(sb.negate(sb.fieldReference(input, 1))),
                table));

    validateAggregateTransformation(rel, expectedFinal);
  }

  @Test
  void handleComplexGroupingArgument() {
    Aggregate rel =
        sb.aggregate(
            input ->
                sb.grouping(
                    sb.fieldReference(input, 2), sb.add(sb.fieldReference(input, 1), sb.i32(42))),
            input -> List.of(),
            table);

    Aggregate expectedFinal =
        sb.aggregate(
            // grouping exprs are now field references to input
            input -> sb.grouping(input, 4, 5),
            input -> List.of(),
            sb.project(
                input ->
                    List.of(
                        sb.fieldReference(input, 2),
                        sb.add(sb.fieldReference(input, 1), sb.i32(42))),
                table));

    validateAggregateTransformation(rel, expectedFinal);
  }

  @Test
  void handleOutOfOrderGroupingArguments() {
    Aggregate rel = sb.aggregate(input -> sb.grouping(input, 1, 0, 2), input -> List.of(), table);

    Aggregate expectedFinal =
        sb.aggregate(
            // grouping exprs are now field references to input
            input -> sb.grouping(input, 4, 5, 6),
            input -> List.of(),
            sb.project(
                // ALL grouping exprs are added to the child projects (including field references)
                input ->
                    List.of(
                        sb.fieldReference(input, 1),
                        sb.fieldReference(input, 0),
                        sb.fieldReference(input, 2)),
                table));

    validateAggregateTransformation(rel, expectedFinal);
  }

  @Test
  void outOfOrderGroupingKeysHaveCorrectCalciteType() {
    Rel rel =
        sb.aggregate(
            input -> sb.grouping(input, 2, 0),
            input -> List.of(),
            sb.namedScan(List.of("foo"), List.of("a", "b", "c"), List.of(R.I64, R.I64, R.STRING)));
    RelNode relNode = substraitToCalcite.convert(rel);
    assertRowMatch(relNode.getRowType(), R.STRING, R.I64);
  }
}
