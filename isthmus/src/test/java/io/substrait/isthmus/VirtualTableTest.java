package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import io.substrait.isthmus.calcite.rel.VirtualTable;
import io.substrait.isthmus.calcite.rel.rules.VirtualTableExpansionRule;
import io.substrait.isthmus.sql.SubstraitSqlDialect;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.VirtualTableScan;
import io.substrait.type.NamedStruct;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

/**
 * The {@link VirtualTable} relation and the rule that expands it: what isthmus emits, what survives
 * a planner, and what the expansion costs.
 */
class VirtualTableTest extends PlanTestBase {

  private final NamedStruct schema =
      NamedStruct.of(List.of("col1", "col2"), R.struct(R.I32, R.FP64));

  private VirtualTableScan computedRows() {
    return virtualTable(
        schema,
        List.of(sb.i32(2), sb.add(sb.fp64(4.4), sb.fp64(4.5))),
        List.of(sb.multiply(sb.i32(6), sb.i32(2)), sb.fp64(8.8)));
  }

  @Test
  void aComputedRowConvertsToTheIsthmusRelation() {
    assertInstanceOf(VirtualTable.class, substraitToCalcite.convert(computedRows()));
  }

  @Test
  void theRuleExpandsTheRowsIntoAUnionOfProjections() {
    RelNode expanded =
        plan(substraitToCalcite.convert(computedRows()), VirtualTableExpansionRule.instance());

    assertEquals(
        "LogicalUnion(all=[true])\n"
            + "  LogicalProject(col1=[2], col2=[+(4.4E0:DOUBLE, 4.5E0:DOUBLE)])\n"
            + "    LogicalValues(tuples=[[{  }]])\n"
            + "  LogicalProject(col1=[*(6, 2)], col2=[8.8E0:DOUBLE])\n"
            + "    LogicalValues(tuples=[[{  }]])\n",
        RelOptUtil.toString(expanded));
  }

  /** One row needs no union, and a planner would strip a one-input one anyway. */
  @Test
  void theRuleExpandsASingleRowIntoAProjection() {
    RelNode expanded =
        plan(
            substraitToCalcite.convert(
                virtualTable(schema, List.of(sb.multiply(sb.i32(6), sb.i32(2)), sb.fp64(8.8)))),
            VirtualTableExpansionRule.instance());

    assertEquals(
        "LogicalProject(col1=[*(6, 2)], col2=[8.8E0:DOUBLE])\n"
            + "  LogicalValues(tuples=[[{  }]])\n",
        RelOptUtil.toString(expanded));
  }

  /**
   * The expansion is one-way, and this is what that costs: what comes back is the relation the
   * expansion is, not the table it came from. That is the reason isthmus does not run the rule
   * itself.
   */
  @Test
  void theExpansionDoesNotConvertBackToAVirtualTable() {
    RelNode expanded =
        plan(substraitToCalcite.convert(computedRows()), VirtualTableExpansionRule.instance());

    assertInstanceOf(
        io.substrait.relation.Set.class, SubstraitRelVisitor.convert(expanded, converterProvider));
  }

  /**
   * The unexpanded relation is opaque to the rules that rewrite the expansion -- UNION_REMOVE
   * strips the one-input union a single-row table would expand to, and PROJECT_MERGE takes a row's
   * projection into whatever sits above it -- so the table is still a table after a planner has run
   * over it.
   */
  @Test
  void theRelationSurvivesPlanning() {
    VirtualTableScan table = computedRows();
    Project project =
        Project.builder().input(table).expressions(List.of(sb.fieldReference(table, 0))).build();

    RelNode planned =
        plan(
            substraitToCalcite.convert(project),
            CoreRules.UNION_REMOVE,
            CoreRules.UNION_MERGE,
            CoreRules.PROJECT_MERGE);

    Rel converted = SubstraitRelVisitor.convert(planned, converterProvider);
    assertEquals(table, assertInstanceOf(Project.class, converted).getInput());
  }

  /**
   * The rows are expressions, and Calcite rewrites a relation's expressions by handing it a
   * shuttle. A relation that does not pass one on keeps its rows out of every rewrite built on
   * that, including the scan that finds the subqueries binding outer references.
   */
  @Test
  void aShuttleReachesTheRows() {
    RelNode table = substraitToCalcite.convert(computedRows());
    RexBuilder rexBuilder = table.getCluster().getRexBuilder();

    RelNode rewritten =
        table.accept(
            new RexShuttle() {
              @Override
              public RexNode visitCall(RexCall call) {
                return call.getOperator().getName().equals("*")
                    ? rexBuilder.makeExactLiteral(BigDecimal.valueOf(12), call.getType())
                    : super.visitCall(call);
              }
            });

    assertEquals(
        "VirtualTable(rows=[[{ 2, +(4.4E0:DOUBLE, 4.5E0:DOUBLE) }, { 12, 8.8E0:DOUBLE }]])\n",
        RelOptUtil.toString(rewritten));
  }

  /**
   * A projection above the table is where the expansion used to lose the schema's names: the
   * renaming projection it carried was merged into this one, and nothing was left to rebuild the
   * table from.
   */
  @Test
  void theTableIsStillATableUnderAProjection() {
    VirtualTableScan table = computedRows();
    Project project =
        Project.builder().input(table).expressions(List.of(sb.fieldReference(table, 0))).build();

    Rel converted =
        SubstraitRelVisitor.convert(substraitToCalcite.convert(project), converterProvider);

    assertEquals(table, assertInstanceOf(Project.class, converted).getInput());
  }

  /**
   * The same where the arms differ in nullability, which is the case a shape match cannot take: the
   * rows come from the arms and the schema from the union's own row type, and the type the union
   * widened to is not the type of either row.
   */
  @Test
  void aHandWrittenUnionOfArmsDifferingInNullabilityStaysAUnion() {
    RelDataType i32 = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RelDataType nullableI32 = typeFactory.createTypeWithNullability(i32, true);
    RexBuilder rexBuilder = builder.getRexBuilder();

    RelNode union =
        LogicalUnion.create(
            List.of(
                singleRowProjection(rexBuilder.makeExactLiteral(BigDecimal.ONE, i32)),
                singleRowProjection(rexBuilder.makeNullLiteral(nullableI32))),
            true);

    Rel converted = SubstraitRelVisitor.convert(union, converterProvider);
    assertInstanceOf(io.substrait.relation.Set.class, converted);
    assertEquals(List.of(N.I32), converted.getRecordType().fields());
  }

  /** The relation stands on its own: it has no inputs, and a copy cannot give it any. */
  @Test
  void theRelationTakesNoInputs() {
    RelNode table = substraitToCalcite.convert(computedRows());

    assertEquals(List.of(), table.getInputs());
    assertEquals(
        RelOptUtil.toString(table),
        RelOptUtil.toString(table.copy(table.getTraitSet(), List.of())));
    assertThrows(
        IllegalArgumentException.class, () -> table.copy(table.getTraitSet(), List.of(table)));
  }

  /**
   * A table of no rows does not reach the relation through a conversion -- a virtual table with no
   * rows has no row that fails to fit a tuple, so it converts to an empty {@code LogicalValues} --
   * but a consumer can build one, and the expansion of no rows is that same empty table.
   */
  @Test
  void theRuleExpandsATableOfNoRowsIntoAnEmptyValues() {
    RelNode table = substraitToCalcite.convert(computedRows());
    RelNode empty = VirtualTable.create(table.getCluster(), table.getRowType(), List.of());

    RelNode expanded = plan(empty, VirtualTableExpansionRule.instance());

    assertEquals("LogicalValues(tuples=[[]])\n", RelOptUtil.toString(expanded));
  }

  /**
   * SQL generation is the consumer the rule exists for: {@link
   * org.apache.calcite.rel.rel2sql.RelToSqlConverter} knows Calcite's own relations only, and
   * throws an {@code AssertionError} naming anything else -- unconditionally, so assertions being
   * off does not help. Both of isthmus' entry points expand before they convert.
   */
  @Test
  void sqlGenerationExpandsTheTable() {
    RelNode table = substraitToCalcite.convert(computedRows());
    io.substrait.plan.Plan plan = sb.plan(sb.root(computedRows(), List.of("col1", "col2")));

    assertAll(
        () ->
            assertEquals(
                "SELECT 2 AS \"col1\", 4.4E0 + 4.5E0 AS \"col2\"\n"
                    + "FROM (VALUES ()) AS \"t\"\n"
                    + "UNION ALL\n"
                    + "SELECT 6 * 2 AS \"col1\", 8.8E0 AS \"col2\"\n"
                    + "FROM (VALUES ()) AS \"t\"",
                SubstraitSqlDialect.toSql(table).getSql()),
        () ->
            assertEquals(
                1,
                new SubstraitToSql(converterProvider)
                    .convert(plan, SubstraitSqlDialect.DEFAULT)
                    .size()));
  }

  /**
   * The rows have to fit the row type: the projection this used to be built as gave that check for
   * free through {@code RexUtil.compatibleTypes}, and {@code AbstractRelNode.isValid} succeeds
   * whatever it is handed.
   */
  @Test
  void theRowsHaveToFitTheRowType() {
    RelNode table = substraitToCalcite.convert(computedRows());
    RelDataType rowType = table.getRowType();
    RexBuilder rexBuilder = table.getCluster().getRexBuilder();
    RexNode i32 =
        rexBuilder.makeExactLiteral(BigDecimal.ONE, typeFactory.createSqlType(SqlTypeName.INTEGER));

    assertAll(
        () ->
            assertThrows(
                IllegalArgumentException.class,
                () -> VirtualTable.create(table.getCluster(), rowType, List.of(List.of(i32)))),
        () ->
            assertThrows(
                IllegalArgumentException.class,
                () ->
                    VirtualTable.create(table.getCluster(), rowType, List.of(List.of(i32, i32)))));
  }

  /**
   * A schema may name two columns the same -- the spec asks only that the names are a depth-first
   * list -- and the table carries them as they are. The expansion cannot: a Calcite projection
   * requires distinct names, so it uniquifies them there rather than failing on a table that
   * converts and round-trips.
   */
  @Test
  void theExpansionUniquifiesRepeatedFieldNames() {
    NamedStruct repeated = NamedStruct.of(List.of("c", "c"), R.struct(R.I32, R.FP64));
    RelNode table =
        substraitToCalcite.convert(
            virtualTable(repeated, List.of(sb.i32(2), sb.add(sb.fp64(4.4), sb.fp64(4.5)))));

    assertEquals(List.of("c", "c"), table.getRowType().getFieldNames());
    assertEquals(
        List.of("c", "c1"),
        plan(table, VirtualTableExpansionRule.instance()).getRowType().getFieldNames());
  }

  /**
   * The relation costs what its expansion costs. The inherited estimate is a row count alone, which
   * is less than the projection per row the rule builds, so a cost-based planner would fire the
   * rule and then keep the relation it started from.
   */
  @Test
  void theRelationCostsWhatItsExpansionCosts() {
    RelNode table = substraitToCalcite.convert(computedRows());
    RelNode expanded = plan(table, VirtualTableExpansionRule.instance());
    RelMetadataQuery mq = table.getCluster().getMetadataQuery();

    assertFalse(
        mq.getCumulativeCost(table).isLt(mq.getCumulativeCost(expanded)),
        mq.getCumulativeCost(table) + " < " + mq.getCumulativeCost(expanded));
  }

  /**
   * A projection of one row over the single empty row, which is what an expanded row looks like.
   */
  private RelNode singleRowProjection(RexNode value) {
    RelDataType emptyRowType = typeFactory.createStructType(List.of(), List.of());
    RelNode emptyRow =
        LogicalValues.create(
            builder.getCluster(), emptyRowType, ImmutableList.of(ImmutableList.of()));
    RelDataType rowType = typeFactory.builder().add("col1", value.getType()).build();
    return LogicalProject.create(
        emptyRow, Collections.emptyList(), List.of(value), rowType, Collections.emptySet());
  }
}
