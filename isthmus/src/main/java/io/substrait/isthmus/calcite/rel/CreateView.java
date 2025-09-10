package io.substrait.isthmus.calcite.rel;

import java.util.List;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;

public class CreateView extends AbstractRelNode {
  private List<String> names;
  private RelRoot input;

  public CreateView(List<String> names, RelRoot input) {
    super(input.rel.getCluster(), input.rel.getTraitSet());
    this.names = names;
    this.input = input;
  }

  @Override
  protected RelDataType deriveRowType() {
    // return new DdlRelDataType();
    return input.validatedRowType;
  }

  public List<String> getNames() {
    return names;
  }

  public void setNames(List<String> names) {
    this.names = names;
  }

  public RelRoot getInput() {
    return input;
  }

  public void setInput(RelRoot input) {
    this.input = input;
  }
}
