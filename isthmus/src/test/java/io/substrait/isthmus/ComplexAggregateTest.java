package io.substrait.isthmus;

import static io.substrait.isthmus.SqlConverterBase.EXTENSION_COLLECTION;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.relation.Aggregate;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ComplexAggregateTest extends PlanTestBase {

  final TypeCreator R = TypeCreator.of(false);
  SubstraitBuilder b = new SubstraitBuilder(extensions);

  Aggregate.Measure withPreMeasureFilter(Aggregate.Measure measure, Expression preMeasureFilter) {
    return Aggregate.Measure.builder().from(measure).preMeasureFilter(preMeasureFilter).build();
  }

  Aggregate.Measure withSort(Aggregate.Measure measure, List<Expression.SortField> sortFields) {
    var afi =
        AggregateFunctionInvocation.builder().from(measure.getFunction()).sort(sortFields).build();
    return Aggregate.Measure.builder().from(measure).function(afi).build();
  }

  /**
   * Check that:
   *
   * <ol>
   *   <li>The {@code pojo} pojo given is transformed as expected by {@link
   *       AggregateValidator.AggregateTransformer#transformToValidCalciteAggregate}
   *   <li>The {@code} (original) pojo can be converted to Calcite without issues
   * </ol>
   *
   * @param pojo a pojo that requires transformation for use in Calcite
   * @param expectedTransform the expected transformation output
   */
  protected void validateAggregateTransformation(Aggregate pojo, Rel expectedTransform) {
    var converterPojo =
        AggregateValidator.AggregateTransformer.transformToValidCalciteAggregate(pojo);
    assertEquals(expectedTransform, converterPojo);

    // Substrait POJO -> Calcite
    new SubstraitToCalcite(EXTENSION_COLLECTION, typeFactory).convert(pojo);
  }

  private List<Type> columnTypes = List.of(R.I32, R.I32, R.I32, R.I32);
  private List<String> columnNames = List.of("a", "b", "c", "d");
  private NamedScan table = b.namedScan(List.of("example"), columnNames, columnTypes);

  private Aggregate.Grouping emptyGrouping = Aggregate.Grouping.builder().build();

  @Test
  void handleComplexMeasureArgument() {
    // SELECT sum(c + 7) FROM example
    var rel =
        b.aggregate(
            input -> emptyGrouping,
            input -> List.of(b.sum(b.add(b.fieldReference(input, 2), b.i32(7)))),
            table);

    var expectedFinal =
        b.aggregate(
            input -> emptyGrouping,
            // sum call references input field
            input -> List.of(b.sum(input, 4)),
            b.project(
                // add call is moved to child project
                input -> List.of(b.add(b.fieldReference(input, 2), b.i32(7))),
                table));

    validateAggregateTransformation(rel, expectedFinal);
  }

  @Test
  void handleComplexPreMeasureFilter() {
    // SELECT sum(a) FILTER (b = 42) FROM example
    var rel =
        b.aggregate(
            input -> emptyGrouping,
            input ->
                List.of(
                    withPreMeasureFilter(
                        b.sum(input, 0), b.equal(b.fieldReference(input, 1), b.i32(42)))),
            table);

    var expectedFinal =
        b.aggregate(
            input -> emptyGrouping,
            input -> List.of(withPreMeasureFilter(b.sum(input, 0), b.fieldReference(input, 4))),
            b.project(input -> List.of(b.equal(b.fieldReference(input, 1), b.i32(42))), table));

    validateAggregateTransformation(rel, expectedFinal);
  }

  @Test
  void handleComplexSortingArguments() {
    // SELECT sum(d ORDER BY -b ASC) FROM example
    var rel =
        b.aggregate(
            input -> emptyGrouping,
            input ->
                List.of(
                    withSort(
                        b.sum(input, 3),
                        List.of(
                            b.sortField(
                                b.negate(b.fieldReference(input, 1)),
                                Expression.SortDirection.ASC_NULLS_FIRST)))),
            table);

    var expectedFinal =
        b.aggregate(
            input -> emptyGrouping,
            input ->
                List.of(
                    withSort(
                        b.sum(input, 3),
                        List.of(
                            b.sortField(
                                b.fieldReference(input, 4),
                                Expression.SortDirection.ASC_NULLS_FIRST)))),
            b.project(
                // negate call is moved to child project
                input -> List.of(b.negate(b.fieldReference(input, 1))),
                table));

    validateAggregateTransformation(rel, expectedFinal);
  }

  @Test
  void handleComplexGroupingArgument() {
    var rel =
        b.aggregate(
            input ->
                b.grouping(
                    b.fieldReference(input, 2), b.add(b.fieldReference(input, 1), b.i32(42))),
            input -> List.of(),
            table);

    var expectedFinal =
        b.aggregate(
            // grouping exprs are now field references to input
            input -> b.grouping(input, 4, 5),
            input -> List.of(),
            b.project(
                input ->
                    List.of(
                        b.fieldReference(input, 2), b.add(b.fieldReference(input, 1), b.i32(42))),
                table));

    validateAggregateTransformation(rel, expectedFinal);
  }

  @Test
  void handleOutOfOrderGroupingArguments() {
    var rel = b.aggregate(input -> b.grouping(input, 1, 0, 2), input -> List.of(), table);

    var expectedFinal =
        b.aggregate(
            // grouping exprs are now field references to input
            input -> b.grouping(input, 4, 5, 6),
            input -> List.of(),
            b.project(
                // ALL grouping exprs are added to the child projects (including field references)
                input ->
                    List.of(
                        b.fieldReference(input, 1),
                        b.fieldReference(input, 0),
                        b.fieldReference(input, 2)),
                table));

    validateAggregateTransformation(rel, expectedFinal);
  }

  @Test
  void outOfOrderGroupingKeysHaveCorrectCalciteType() {
    Rel rel =
        b.aggregate(
            input -> b.grouping(input, 2, 0),
            input -> List.of(),
            b.namedScan(List.of("foo"), List.of("a", "b", "c"), List.of(R.I64, R.I64, R.STRING)));
    var relNode = new SubstraitToCalcite(EXTENSION_COLLECTION, typeFactory).convert(rel);
    assertRowMatch(relNode.getRowType(), R.STRING, R.I64);
  }
}
