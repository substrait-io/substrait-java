package io.substrait.isthmus.calcite.rel;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;

public class CreateView extends SingleRel {
  private final List<String> viewName;

  private CreateView(
      RelOptCluster cluster, RelTraitSet traitSet, List<String> viewName, RelNode input) {
    super(cluster, traitSet, input);
    this.viewName = viewName;
  }

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
   * Returns the inputs to this node (single input).
   *
   * @return a list containing the input relation
   */
  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.size() == 1;
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
