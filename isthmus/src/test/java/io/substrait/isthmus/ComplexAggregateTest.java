package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableAggregateFunctionInvocation;
import io.substrait.relation.Aggregate;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
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
  void roundTripPreMeasureFilter() {
    NamedScan input =
        sb.namedScan(List.of("example"), List.of("value", "filter"), List.of(R.I32, R.BOOLEAN));
    Aggregate rel =
        sb.aggregate(
            aggregateInput -> emptyGrouping,
            aggregateInput ->
                List.of(
                    withPreMeasureFilter(
                        sb.sum(aggregateInput, 0), sb.fieldReference(aggregateInput, 1))),
            input);

    assertFullRoundTrip(rel);
  }

  @Test
  void roundTripSortingArguments() {
    Aggregate rel =
        sb.aggregate(
            input -> emptyGrouping,
            input ->
                List.of(
                    withSort(
                        sb.sum(input, 3),
                        List.of(
                            sb.sortField(
                                sb.fieldReference(input, 1),
                                Expression.SortDirection.ASC_NULLS_FIRST),
                            sb.sortField(
                                sb.fieldReference(input, 2),
                                Expression.SortDirection.DESC_NULLS_LAST)))),
            table);

    assertFullRoundTrip(rel);
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
  void outOfOrderGroupingSetsHaveCorrectCalciteType() {
    // Each grouping set holds one field and is trivially in order, but the aggregate declares
    // field 2 before field 0, while Calcite emits its grouping columns in ascending field order.
    Rel rel =
        sb.aggregate(
            input -> List.of(sb.grouping(input, 2), sb.grouping(input, 0)),
            input -> List.of(),
            Optional.of(Rel.Remap.of(List.of(0, 1))),
            sb.namedScan(List.of("foo"), List.of("a", "b", "c"), List.of(R.I64, R.I64, R.STRING)));

    RelNode relNode = substraitToCalcite.convert(rel);

    assertRowMatch(relNode.getRowType(), N.STRING, N.I64);
  }

  @Test
  void groupingFieldSharedBySetsStaysOneColumn() {
    // Field 2 is grouped on twice. It is one column of the aggregate's output, so it has to stay
    // one column of the project the conversion puts underneath it.
    Rel rel =
        sb.aggregate(
            input -> List.of(sb.grouping(input, 2, 0), sb.grouping(input, 2)),
            input -> List.of(),
            Optional.of(Rel.Remap.of(List.of(0, 1))),
            sb.namedScan(List.of("foo"), List.of("a", "b", "c"), List.of(R.I64, R.I64, R.STRING)));

    RelNode relNode = substraitToCalcite.convert(rel);

    assertRowMatch(relNode.getRowType(), R.STRING, N.I64);
  }

  /**
   * A relation that keeps its grouping-set index maps it to the column the conversion adds for it,
   * which sits after the grouping columns and the measures. Calcite folds the {@code GROUP_ID} call
   * into a literal, so that is what the column holds -- which value it holds is a separate question
   * from which column it is.
   */
  @Test
  void theGroupingSetIndexIsTheColumnTheConversionAddedForIt() {
    Rel aggregate =
        sb.aggregate(
            input -> List.of(sb.grouping(input, 2), sb.grouping(input, 0)),
            input -> List.of(sb.count(input, 0)),
            Optional.of(Rel.Remap.of(List.of(0, 1, 2, 3))),
            sb.namedScan(List.of("foo"), List.of("a", "b", "c"), List.of(R.I64, R.I64, R.STRING)));

    RelNode relNode = substraitToCalcite.convert(aggregate);

    assertEquals(
        "LogicalProject(c=[$1], a=[$0], $f2=[$2], $f3=[0:BIGINT])\n"
            + "  LogicalAggregate(group=[{0, 2}], groups=[[{0}, {2}]], agg#0=[COUNT($0)])\n"
            + "    LogicalTableScan(table=[[foo]])\n",
        RelOptUtil.toString(relNode));
  }

  /**
   * Field 0 is grouped on by both sets and is one column of the output, so the grouping-set index
   * is the fourth column and not the fifth. Counting every mention of a grouping expression put it
   * past the end, and the mapping then kept an index the converted aggregate did not have.
   */
  @Test
  void aGroupingFieldSharedBySetsLeavesTheGroupingSetIndexWhereItIs() {
    Rel aggregate =
        sb.aggregate(
            input -> List.of(sb.grouping(input, 0, 2), sb.grouping(input, 0)),
            input -> List.of(sb.count(input, 0)),
            Optional.of(Rel.Remap.of(List.of(0, 1, 2, 3))),
            sb.namedScan(List.of("foo"), List.of("a", "b", "c"), List.of(R.I64, R.I64, R.STRING)));

    RelNode relNode = substraitToCalcite.convert(aggregate);

    assertEquals(
        "LogicalProject(a=[$0], c=[$1], $f2=[$2], $f3=[0:BIGINT])\n"
            + "  LogicalAggregate(group=[{0, 2}], groups=[[{0, 2}, {0}]], agg#0=[COUNT($0)])\n"
            + "    LogicalTableScan(table=[[foo]])\n",
        RelOptUtil.toString(relNode));
  }

  @Test
  void aReferenceOverOutOfOrderGroupingSetsReachesTheColumnItNames() {
    Rel aggregate =
        sb.aggregate(
            input -> List.of(sb.grouping(input, 2), sb.grouping(input, 0)),
            input -> List.of(),
            Optional.empty(),
            sb.namedScan(List.of("foo"), List.of("a", "b", "c"), List.of(R.I64, R.I64, R.STRING)));
    // Field 0 of the aggregate is the field it groups on first, the string.
    Rel project =
        Project.builder()
            .input(aggregate)
            .remap(Rel.Remap.offset(3, 1))
            .addExpressions(sb.fieldReference(aggregate, 0))
            .build();

    RelNode relNode = substraitToCalcite.convert(project);

    assertRowMatch(relNode.getRowType(), N.STRING);
  }

  @Test
  void anAggregateOverOutOfOrderGroupingSetsRoundTrips() {
    // The grouping columns survive the trip in the order the aggregate declares them, rather than
    // in the order Calcite happens to emit them. Only those columns are compared: the grouping-set
    // index comes back as an i64, because the conversion builds Calcite's GROUP_ID call as a
    // BIGINT and Calcite folds it to a literal of that type, which is a separate difference.
    Rel aggregate =
        sb.aggregate(
            input -> List.of(sb.grouping(input, 2), sb.grouping(input, 0)),
            input -> List.of(),
            Optional.empty(),
            sb.namedScan(List.of("foo"), List.of("a", "b", "c"), List.of(R.I64, R.I64, R.STRING)));

    RelNode relNode = substraitToCalcite.convert(aggregate);
    Rel converted =
        SubstraitRelVisitor.convert(RelRoot.of(relNode, SqlKind.SELECT), converterProvider)
            .getInput();

    List<Type> declared = aggregate.getRecordType().fields();
    List<Type> roundTripped = converted.getRecordType().fields();
    assertEquals(declared.size(), roundTripped.size());
    assertEquals(declared.subList(0, 2), roundTripped.subList(0, 2));
  }

  @Test
  void anExplicitGroupIdCallKeepsTheDeclaredColumnOrder() {
    // Calcite folds GROUP_ID() into a literal wherever it can work out the answer, so a plan that
    // still carries the call has to be built rather than parsed. Its grouping sets mention field 3
    // before field 2, which is the order the converted relation has to declare its columns in --
    // the shape a query whose grouping sets are followed by another key produces.
    RelBuilder relBuilder = new RelCreator(TPCH_CATALOG).createRelBuilder();
    RelNode scan = relBuilder.scan("LINEITEM").build();
    AggregateCall groupId =
        AggregateCall.create(
            SqlStdOperatorTable.GROUP_ID,
            false,
            false,
            false,
            List.of(),
            List.of(),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            null);
    RelNode calciteAggregate =
        LogicalAggregate.create(
            scan,
            List.of(),
            ImmutableBitSet.of(0, 1, 2, 3),
            List.of(ImmutableBitSet.of(0, 1, 3), ImmutableBitSet.of(2, 3)),
            List.of(groupId));

    Rel rel =
        SubstraitRelVisitor.convert(RelRoot.of(calciteAggregate, SqlKind.SELECT), converterProvider)
            .getInput();

    // The mapping is what carries the difference, and it is asserted directly: the sets mention
    // fields 0, 1 and 3 before 2, so the relation declares them in that order, while the aggregate
    // underneath emits them by field index. Types alone would not show it -- three of these four
    // columns are BIGINT.
    assertEquals(Optional.of(Rel.Remap.of(List.of(0, 1, 3, 2, 4))), ((Aggregate) rel).getRemap());

    // What the relation says it emits is what the Calcite aggregate it came from emits. The
    // grouping-set index is left out of the comparison: Calcite types its GROUP_ID column BIGINT
    // while Substrait gives the aggregate an i32 one, which is a difference of its own.
    List<Type> emitted = rel.getRecordType().fields();
    assertEquals(5, emitted.size());
    assertRowMatch(
        typeFactory.createStructType(calciteAggregate.getRowType().getFieldList().subList(0, 4)),
        emitted.subList(0, 4));
  }

  /**
   * Every other fixture here swaps two columns, and a transposition is its own inverse, so a
   * mapping replaced by the one that undoes it would go unnoticed. These two sets mention fields 0
   * and 3 before 1 and 2, which makes the mapping a three-cycle: the relation declares (a, d, b, c)
   * where the aggregate underneath emits (a, b, c, d), and the inverse would declare (a, c, d, b).
   */
  @Test
  void anAggregateOverGroupingSetsInANonSwapOrderKeepsTheDeclaredOrder() {
    Rel aggregate =
        sb.aggregate(
            input -> List.of(sb.grouping(input, 0, 3), sb.grouping(input, 1, 2)),
            input -> List.of(),
            Optional.empty(),
            sb.namedScan(
                List.of("foo"),
                List.of("a", "b", "c", "d"),
                List.of(R.I64, R.STRING, R.FP64, R.BOOLEAN)));

    RelNode relNode = substraitToCalcite.convert(aggregate);

    assertEquals(
        "LogicalProject(a=[$0], d=[$3], b=[$1], c=[$2], $f4=[0:BIGINT])\n"
            + "  LogicalAggregate(group=[{0, 1, 2, 3}], groups=[[{0, 3}, {1, 2}]])\n"
            + "    LogicalTableScan(table=[[foo]])\n",
        RelOptUtil.toString(relNode));
  }

  /** The same shape in the other direction, asserted on the mapping the conversion produces. */
  @Test
  void groupingSetsInANonSwapOrderGiveAMappingThatIsNotItsOwnInverse() {
    RelBuilder relBuilder = new RelCreator(TPCH_CATALOG).createRelBuilder();
    RelNode scan = relBuilder.scan("LINEITEM").build();
    RelNode calciteAggregate =
        LogicalAggregate.create(
            scan,
            List.of(),
            ImmutableBitSet.of(0, 1, 2, 3),
            List.of(ImmutableBitSet.of(0, 3), ImmutableBitSet.of(1, 2)),
            List.of());

    Rel rel =
        SubstraitRelVisitor.convert(RelRoot.of(calciteAggregate, SqlKind.SELECT), converterProvider)
            .getInput();

    assertEquals(Optional.of(Rel.Remap.of(List.of(0, 2, 3, 1))), ((Aggregate) rel).getRemap());
  }

  /**
   * A lone grouping set gives every mention of an expression a column of its own -- {@code
   * Aggregate.deriveRecordType} dedups the grouping expressions only across several sets -- while
   * Calcite's grouping bit set cannot hold a field twice. So a repeated field reaches Calcite as
   * two columns of the projection the conversion puts underneath the aggregate.
   */
  @Test
  void aGroupingSetNamingAFieldTwiceKeepsAColumnPerMention() {
    Rel scan =
        sb.namedScan(List.of("foo"), List.of("a", "b", "c"), List.of(R.I64, R.I64, R.STRING));

    RelNode relNode =
        substraitToCalcite.convert(
            sb.aggregate(input -> sb.grouping(input, 2, 0, 2), input -> List.of(), scan));

    assertRowMatch(relNode.getRowType(), R.STRING, R.I64, R.STRING);
  }

  /** The same repeat under an emit mapping, whose indices count the columns the aggregate holds. */
  @Test
  void aGroupingSetNamingAFieldTwiceUnderAnEmitMappingKeepsAColumnPerMention() {
    Rel scan =
        sb.namedScan(List.of("foo"), List.of("a", "b", "c"), List.of(R.I64, R.I64, R.STRING));

    RelNode relNode =
        substraitToCalcite.convert(
            sb.aggregate(
                input -> List.of(sb.grouping(input, 2, 0, 2)),
                input -> List.of(),
                Optional.of(Rel.Remap.of(List.of(2, 0))),
                scan));

    assertRowMatch(relNode.getRowType(), R.STRING, R.STRING);
  }

  /** A repeat the grouping fields are in ascending order for reaches Calcite the same way. */
  @Test
  void anAscendingGroupingSetNamingAFieldTwiceKeepsAColumnPerMention() {
    Rel scan =
        sb.namedScan(List.of("foo"), List.of("a", "b", "c"), List.of(R.I64, R.I64, R.STRING));

    RelNode relNode =
        substraitToCalcite.convert(
            sb.aggregate(input -> sb.grouping(input, 0, 2, 2), input -> List.of(), scan));

    assertRowMatch(relNode.getRowType(), R.I64, R.STRING, R.STRING);
  }

  /** And so does a repeat of an expression the transformer has to project out anyway. */
  @Test
  void aGroupingSetNamingAnExpressionTwiceKeepsAColumnPerMention() {
    Rel scan =
        sb.namedScan(List.of("foo"), List.of("a", "b", "c"), List.of(R.I64, R.I64, R.STRING));

    RelNode relNode =
        substraitToCalcite.convert(
            sb.aggregate(
                input ->
                    sb.grouping(
                        sb.add(sb.fieldReference(input, 0), sb.i64(42)),
                        sb.add(sb.fieldReference(input, 0), sb.i64(42))),
                input -> List.of(),
                scan));

    assertRowMatch(relNode.getRowType(), R.I64, R.I64);
  }

  /**
   * The order the fields are grouped in is carried by the emit mapping, so it is no longer a reason
   * to rewrite the input: the aggregate reads the relation it was given, and the declared order is
   * a projection above it rather than below.
   */
  @Test
  void outOfOrderGroupingKeysLeaveTheInputAlone() {
    Rel scan =
        sb.namedScan(List.of("foo"), List.of("a", "b", "c"), List.of(R.I64, R.I64, R.STRING));

    RelNode relNode =
        substraitToCalcite.convert(
            sb.aggregate(input -> sb.grouping(input, 2, 0), input -> List.of(), scan));

    assertEquals(
        "LogicalProject(c=[$1], a=[$0])\n"
            + "  LogicalAggregate(group=[{0, 2}])\n"
            + "    LogicalTableScan(table=[[foo]])\n",
        RelOptUtil.toString(relNode));
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

  /**
   * A grouping expression that is not a field reference into the aggregate's input -- an outer
   * reference, which the pre-Calcite transform leaves alone rather than projecting out -- is put by
   * Calcite in a projection after the input's own fields, so it is emitted last however early the
   * aggregate declares it. The emit mapping has to follow it there.
   */
  @Test
  void outOfOrderGroupingSetsOverAnOuterReference() {
    Rel outer = sb.namedScan(List.of("bar"), List.of("x"), List.of(R.I64)).withRelAnchor(1);
    Rel inner =
        sb.namedScan(List.of("foo"), List.of("a", "b", "c"), List.of(R.I64, R.I64, R.STRING));

    Aggregate aggregate =
        Aggregate.builder()
            .input(inner)
            .addGroupings(
                Aggregate.Grouping.builder()
                    .addExpressions(
                        FieldReference.newRootStructOuterReferenceByRelReference(0, R.I64, 1))
                    .build())
            .addGroupings(
                Aggregate.Grouping.builder().addExpressions(sb.fieldReference(inner, 2)).build())
            // The first grouping column the aggregate declares, which is the outer reference.
            .remap(Rel.Remap.of(List.of(0)))
            .build();

    Rel root =
        sb.project(
            input -> List.of(sb.scalarSubquery(aggregate, N.I64)), Rel.Remap.of(List.of(1)), outer);

    RelNode relNode = substraitToCalcite.convert(root);

    assertRowMatch(relNode.getRowType(), N.I64);
  }
}
