package io.substrait.isthmus.operation;

import java.util.List;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;

public abstract class CreateWithInput extends Create {
  private final RelRoot input;

  public CreateWithInput(RelationType relationType, List<String> names, RelRoot input) {
    super(relationType, names);
    this.input = input;
  }

  public RelRoot getInput() {
    return input;
  }

  public RelDataType getRowType() {
    return input.rel.getRowType();
  }

  @Override
  public <R> R accept(CalciteOperationVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
