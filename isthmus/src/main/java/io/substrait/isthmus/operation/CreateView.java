package io.substrait.isthmus.operation;

import java.util.List;
import org.apache.calcite.rel.RelRoot;

public class CreateView extends CreateWithInput {
  public CreateView(List<String> names, RelRoot input) {
    super(RelationType.VIEW, names, input);
  }

  @Override
  public <R> R accept(CalciteOperationVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
