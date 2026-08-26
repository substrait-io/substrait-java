package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.Expression;
import io.substrait.isthmus.calcite.rel.VirtualTable;
import io.substrait.isthmus.calcite.rel.rules.VirtualTableExpansionRule;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.VirtualTableScan;
import io.substrait.type.NamedStruct;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
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
        plan(substraitToCalcite.convert(computedRows()), VirtualTableExpansionRule.INSTANCE);

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
                virtualTable(List.of(sb.multiply(sb.i32(6), sb.i32(2)), sb.fp64(8.8)))),
            VirtualTableExpansionRule.INSTANCE);

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
        plan(substraitToCalcite.convert(computedRows()), VirtualTableExpansionRule.INSTANCE);

    assertInstanceOf(
        io.substrait.relation.Set.class, SubstraitRelVisitor.convert(expanded, extensions));
  }

  /**
   * The unexpanded relation is opaque to the rules that rewrite the expansion -- UNION_REMOVE
   * strips the one-input union a single-row table would expand to, and PROJECT_MERGE takes a row's
   * projection into whatever sits above it -- so the table is still a table after a planner has run
   * over it.
   */
  @Test
  void theRelationSurvivesPlanning() {
    RelNode planned =
        plan(
            substraitToCalcite.convert(computedRows()),
            CoreRules.UNION_REMOVE,
            CoreRules.UNION_MERGE,
            CoreRules.PROJECT_MERGE);

    assertInstanceOf(VirtualTable.class, planned);
    assertEquals(computedRows(), SubstraitRelVisitor.convert(planned, extensions));
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

    Rel converted = SubstraitRelVisitor.convert(substraitToCalcite.convert(project), extensions);

    assertEquals(table, assertInstanceOf(Project.class, converted).getInput());
  }

  /**
   * A union someone wrote out of single-row projections is a union. It converts to the same Calcite
   * tree the expansion does, so nothing in the tree can tell the two apart -- which is why the
   * table is recognised by its own type instead.
   */
  @Test
  void aHandWrittenUnionOfSingleRowProjectionsStaysAUnion() {
    RelDataType i32 = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RexBuilder rexBuilder = builder.getRexBuilder();

    RelNode union =
        LogicalUnion.create(
            List.of(
                singleRowProjection(rexBuilder.makeExactLiteral(BigDecimal.ONE, i32)),
                singleRowProjection(rexBuilder.makeExactLiteral(BigDecimal.valueOf(2), i32))),
            true);

    assertInstanceOf(
        io.substrait.relation.Set.class, SubstraitRelVisitor.convert(union, extensions));
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

    Rel converted = SubstraitRelVisitor.convert(union, extensions);
    assertInstanceOf(io.substrait.relation.Set.class, converted);
    assertEquals(List.of(N.I32), converted.getRecordType().fields());
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

  @SafeVarargs
  private VirtualTableScan virtualTable(List<Expression>... rows) {
    List<Expression.NestedStruct> structs =
        Arrays.stream(rows)
            .map(row -> Expression.NestedStruct.builder().addAllFields(row).build())
            .collect(Collectors.toList());
    return VirtualTableScan.builder().initialSchema(schema).addAllRows(structs).build();
  }

  private RelNode plan(RelNode rel, RelOptRule... rules) {
    HepProgramBuilder program = new HepProgramBuilder();
    for (RelOptRule rule : rules) {
      program.addRuleInstance(rule);
    }
    HepPlanner planner = new HepPlanner(program.build());
    planner.setRoot(rel);
    return planner.findBestExp();
  }
}
