package io.substrait.isthmus.calcite.rel;

import java.util.List;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;

public class CreateTable extends AbstractRelNode {

  private final List<String> names;
  private final RelRoot input;

  public CreateTable(List<String> names, RelRoot input) {
    super(input.rel.getCluster(), input.rel.getTraitSet());

    this.names = names;
    this.input = input;
  }

  @Override
  protected RelDataType deriveRowType() {
    return input.validatedRowType;
  }

  public List<String> getNames() {
    return names;
  }

  public RelRoot getInput() {
    return input;
  }
}
