package io.substrait.isthmus.calcite.rel.rules;

import com.google.common.collect.ImmutableList;
import io.substrait.isthmus.calcite.rel.VirtualTable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
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

  /**
   * Returns the rule instance to add to a planner.
   *
   * <p>Held by a nested class rather than a field of this one: the configuration builds a rule and
   * the rule reads its configuration, so a field here would have the two initialize each other.
   *
   * @return the rule instance
   */
  public static VirtualTableExpansionRule instance() {
    return InstanceHolder.INSTANCE;
  }

  private static final class InstanceHolder {
    private static final VirtualTableExpansionRule INSTANCE = Config.DEFAULT.toRule();
  }

  private VirtualTableExpansionRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    call.transformTo(expand(call.rel(0)));
  }

  /**
   * Expands every {@link VirtualTable} in the given tree, leaving the rest of it alone.
   *
   * <p>For a consumer that has to hand the tree to something knowing only Calcite's own relations.
   * Isthmus' own SQL generation is one: {@link org.apache.calcite.rel.rel2sql.RelToSqlConverter}
   * has no case for a relation it does not know and throws an {@link AssertionError} naming it.
   *
   * @param relNode the tree to expand
   * @return the tree with every virtual table expanded
   */
  public static RelNode expandAll(RelNode relNode) {
    HepPlanner planner =
        new HepPlanner(new HepProgramBuilder().addRuleInstance(instance()).build());
    planner.setRoot(relNode);
    return planner.findBestExp();
  }

  private static RelNode expand(VirtualTable virtualTable) {
    RelOptCluster cluster = virtualTable.getCluster();
    RelDataType rowType = virtualTable.getRowType();
    if (virtualTable.getRows().isEmpty()) {
      return LogicalValues.create(cluster, rowType, ImmutableList.of());
    }

    RelDataType emptyRowType = cluster.getTypeFactory().createStructType(List.of(), List.of());
    ImmutableList<ImmutableList<RexLiteral>> singleEmptyRow = ImmutableList.of(ImmutableList.of());
    // One empty row for every projection: they share a digest, so a planner keeps one of them
    // whether the rule builds one or many.
    RelNode emptyRow = LogicalValues.create(cluster, emptyRowType, singleEmptyRow);

    // A projection carries the variables the rows resolve against: a subquery among them is
    // unreachable by SubQueryRemoveRule and RelDecorrelator, and the expansion is what a consumer
    // hands to a planner that only knows Calcite's own relations.
    List<String> fieldNames =
        SqlValidatorUtil.uniquify(
            rowType.getFieldNames(), SqlValidatorUtil.F_SUGGESTER, /* caseSensitive= */ true);
    RelDataType projectRowType =
        cluster
            .getTypeFactory()
            .createStructType(
                rowType.getFieldList().stream()
                    .map(RelDataTypeField::getType)
                    .collect(Collectors.toList()),
                fieldNames);

    List<RelNode> rowProjects = new ArrayList<>();
    for (List<RexNode> row : virtualTable.getRows()) {
      rowProjects.add(
          LogicalProject.create(
              emptyRow,
              Collections.emptyList(),
              row,
              projectRowType,
              virtualTable.getVariablesSet()));
    }
    // A one-input union is not a relation a planner keeps -- UNION_REMOVE strips it -- and the
    // projection is what the expansion means anyway.
    return rowProjects.size() == 1 ? rowProjects.get(0) : LogicalUnion.create(rowProjects, true);
  }

  /**
   * Rule configuration.
   *
   * <p>Written out rather than generated: the generated implementation copies {@link
   * RelRule.Config#description()}, which Calcite declares nullable, and so imports {@code
   * javax.annotation.Nullable} -- which isthmus does not have on its compile classpath. The
   * annotation lands in the builder method that copies from the supertype, where {@code
   * Value.Style}'s {@code allowedClasspathAnnotations}, {@code nullableAnnotation} and {@code
   * fallbackNullableAnnotation} do not reach it.
   *
   * <p>{@link #relBuilderFactory()} is inert: the expansion is built directly and never asks the
   * call for a builder. The interface requires an answer, so this is Calcite's own default.
   */
  public static class Config implements RelRule.Config {

    private static final OperandTransform TABLE =
        operand -> operand.operand(VirtualTable.class).noInputs();

    /** The configuration {@link VirtualTableExpansionRule#instance()} is built from. */
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
