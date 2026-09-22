package io.substrait.isthmus.calcite.rel.rules;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.substrait.isthmus.calcite.rel.VirtualTable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
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
 * not the virtual table it came from. Isthmus never runs it as a planner rule -- a plan it converts
 * keeps the {@link VirtualTable}, which is what makes the round trip exact -- and its SQL generator
 * takes only the shape, through {@link #expand(VirtualTable)}.
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
   * Expands one virtual table into the projections its rows stand for.
   *
   * <p>The shape both this rule and isthmus' SQL generation use, so that the two cannot drift: the
   * generator has its own case for the relation rather than a planner pass, and takes the expansion
   * from here.
   *
   * @param virtualTable the table to expand
   * @return the union of one projection per row, or the projection itself where there is one row
   */
  public static RelNode expand(VirtualTable virtualTable) {
    RelOptCluster cluster = virtualTable.getCluster();
    RelDataType rowType = virtualTable.getRowType();
    // A schema may name two columns the same -- the spec asks only that the names are a depth-first
    // list -- and the table carries them as they are. Calcite's own relations cannot, so both
    // branches below build their row type from the uniquified names.
    List<String> fieldNames =
        SqlValidatorUtil.uniquify(
            rowType.getFieldNames(), SqlValidatorUtil.F_SUGGESTER, /* caseSensitive= */ true);
    RelDataType uniquifiedRowType =
        cluster
            .getTypeFactory()
            .createStructType(
                rowType.getFieldList().stream()
                    .map(RelDataTypeField::getType)
                    .collect(Collectors.toList()),
                fieldNames);

    if (virtualTable.getRows().isEmpty()) {
      return LogicalValues.create(cluster, uniquifiedRowType, ImmutableList.of());
    }

    RelDataType emptyRowType = cluster.getTypeFactory().createStructType(List.of(), List.of());
    ImmutableList<ImmutableList<RexLiteral>> singleEmptyRow = ImmutableList.of(ImmutableList.of());
    // One empty row for every projection: they share a digest, so a planner keeps one of them
    // whether the rule builds one or many.
    RelNode emptyRow = LogicalValues.create(cluster, emptyRowType, singleEmptyRow);

    List<RelNode> rowProjects = new ArrayList<>();
    for (List<RexNode> row : virtualTable.getRows()) {
      rowProjects.add(
          LogicalProject.create(
              emptyRow,
              Collections.emptyList(),
              row,
              uniquifiedRowType,
              // Not the table's own set: a leaf declares none, and a projection over a zero-field
              // Values cannot resolve one anyway.
              ImmutableSet.of()));
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
