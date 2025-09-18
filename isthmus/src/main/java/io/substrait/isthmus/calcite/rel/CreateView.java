package io.substrait.isthmus.calcite.rel;

import java.util.List;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;

public class CreateView extends AbstractRelNode {
  private final List<String> viewName;
  private final RelRoot input;

  public CreateView(List<String> viewName, RelRoot input) {
    super(input.rel.getCluster(), input.rel.getTraitSet());
    this.viewName = viewName;
    this.input = input;
  }

  @Override
  protected RelDataType deriveRowType() {
    return input.validatedRowType;
  }

  public List<String> getViewName() {
    return viewName;
  }

  public RelRoot getInput() {
    return input;
  }
}
