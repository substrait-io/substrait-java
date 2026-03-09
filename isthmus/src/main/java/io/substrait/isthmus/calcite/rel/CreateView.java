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

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("viewName", getViewName());
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.size() == 1;
    return new CreateView(getCluster(), traitSet, viewName, inputs.get(0));
  }

  public List<String> getViewName() {
    return viewName;
  }
}
