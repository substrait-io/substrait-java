package io.substrait.isthmus.calcite.rel;

import java.util.List;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;

public class CreateView extends AbstractRelNode {
  private final List<String> viewName;
  private final RelNode input;

  public CreateView(final List<String> viewName, final RelNode input) {
    super(input.getCluster(), input.getTraitSet());
    this.viewName = viewName;
    this.input = input;
  }

  @Override
  protected RelDataType deriveRowType() {
    return input.getRowType();
  }

  @Override
  public RelWriter explainTerms(final RelWriter pw) {
    return super.explainTerms(pw).input("input", getInput()).item("viewName", getViewName());
  }

  @Override
  public List<RelNode> getInputs() {
    return List.of(input);
  }

  public List<String> getViewName() {
    return viewName;
  }

  public RelNode getInput() {
    return input;
  }
}
