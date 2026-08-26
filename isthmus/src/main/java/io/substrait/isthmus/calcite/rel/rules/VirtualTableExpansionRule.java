package io.substrait.isthmus.calcite.rel.rules;

import com.google.common.collect.ImmutableList;
import io.substrait.isthmus.calcite.rel.VirtualTable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Expands a {@link VirtualTable} into stock Calcite relations: a projection computing each row over
 * a single empty row, and a UNION ALL of those where there is more than one.
 *
 * <pre>{@code
 * VirtualTable(rows=[{ e1, e2 }, { e3, e4 }])
 *
 *   LogicalUnion(all=[true])
 *     LogicalProject(exprs=[e1, e2])
 *       LogicalValues(tuples=[[{ }]])
 *     LogicalProject(exprs=[e3, e4])
 *       LogicalValues(tuples=[[{ }]])
 * }</pre>
 *
 * <p>This is for a consumer whose planner only knows Calcite's own relations. It is one-way: the
 * expansion is a plan like any other, and converting it back to Substrait gives the relation it is,
 * not the virtual table it came from. Isthmus never runs it -- a plan it converts keeps the {@link
 * VirtualTable}, which is what makes the round trip exact.
 */
public class VirtualTableExpansionRule extends RelRule<VirtualTableExpansionRule.Config> {

  /** The rule instance to add to a planner. */
  public static final VirtualTableExpansionRule INSTANCE = Config.DEFAULT.toRule();

  private VirtualTableExpansionRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    call.transformTo(expand(call.rel(0)));
  }

  private static RelNode expand(VirtualTable virtualTable) {
    RelOptCluster cluster = virtualTable.getCluster();
    RelDataType rowType = virtualTable.getRowType();
    if (virtualTable.getRows().isEmpty()) {
      return LogicalValues.create(cluster, rowType, ImmutableList.of());
    }

    RelDataType emptyRowType = cluster.getTypeFactory().createStructType(List.of(), List.of());
    ImmutableList<ImmutableList<RexLiteral>> singleEmptyRow = ImmutableList.of(ImmutableList.of());

    List<RelNode> rowProjects = new ArrayList<>();
    for (List<RexNode> row : virtualTable.getRows()) {
      RelNode emptyRow = LogicalValues.create(cluster, emptyRowType, singleEmptyRow);
      rowProjects.add(
          LogicalProject.create(
              emptyRow, Collections.emptyList(), row, rowType, Collections.emptySet()));
    }
    // A one-input union is not a relation a planner keeps -- UNION_REMOVE strips it -- and the
    // projection is what the expansion means anyway.
    return rowProjects.size() == 1 ? rowProjects.get(0) : LogicalUnion.create(rowProjects, true);
  }

  /**
   * Rule configuration.
   *
   * <p>Written out rather than generated: the rule matches one relation and has nothing to
   * configure, so the three properties {@link RelRule.Config} declares are all there is.
   */
  public static class Config implements RelRule.Config {

    private static final OperandTransform TABLE =
        operand -> operand.operand(VirtualTable.class).noInputs();

    /** The configuration {@link VirtualTableExpansionRule#INSTANCE} is built from. */
    public static final Config DEFAULT =
        new Config(RelFactories.LOGICAL_BUILDER, "VirtualTableExpansionRule", TABLE);

    private final RelBuilderFactory relBuilderFactory;
    private final String description;
    private final OperandTransform operandSupplier;

    private Config(
        RelBuilderFactory relBuilderFactory, String description, OperandTransform operandSupplier) {
      this.relBuilderFactory = relBuilderFactory;
      this.description = description;
      this.operandSupplier = operandSupplier;
    }

    @Override
    public VirtualTableExpansionRule toRule() {
      return new VirtualTableExpansionRule(this);
    }

    @Override
    public RelBuilderFactory relBuilderFactory() {
      return relBuilderFactory;
    }

    @Override
    public Config withRelBuilderFactory(RelBuilderFactory factory) {
      return new Config(factory, description, operandSupplier);
    }

    @Override
    public String description() {
      return description;
    }

    @Override
    public Config withDescription(String description) {
      return new Config(relBuilderFactory, description, operandSupplier);
    }

    @Override
    public OperandTransform operandSupplier() {
      return operandSupplier;
    }

    @Override
    public Config withOperandSupplier(OperandTransform transform) {
      return new Config(relBuilderFactory, description, transform);
    }
  }
}
