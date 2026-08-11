package io.substrait.isthmus.calcite.rel;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;

/** Represents a CREATE VIEW DDL operation in Calcite's relational algebra. */
public class CreateView extends SingleRel {
  private final List<String> viewName;

  private CreateView(
      RelOptCluster cluster, RelTraitSet traitSet, List<String> viewName, RelNode input) {
    super(cluster, traitSet, input);
    this.viewName = viewName;
  }

  /**
   * CreateView Constructor.
   *
   * @param viewName view name components
   * @param input RelNode input
   */
  public CreateView(List<String> viewName, RelNode input) {
    this(input.getCluster(), input.getTraitSet(), viewName, input);
  }

  /**
   * Explains the node terms for plan output.
   *
   * @param pw plan writer
   * @return the plan writer with this node's fields added
   */
  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("viewName", getViewName());
  }

  /**
   * Copies this node with the given traits and input.
   *
   * @param traitSet the RelTraitSet
   * @param inputs List of RelNodes
   * @return a copy of this node with the given input
   * @throws IllegalArgumentException if given anything but exactly one input
   */
  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    if (inputs.size() != 1) {
      throw new IllegalArgumentException(
          "CreateView requires exactly one input, but got " + inputs.size());
    }
    return new CreateView(getCluster(), traitSet, viewName, inputs.get(0));
  }

  /**
   * Returns the fully qualified view name parts.
   *
   * @return view name components (e.g., [schema, view])
   */
  public List<String> getViewName() {
    return viewName;
  }
}
