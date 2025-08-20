package io.substrait.isthmus.operation;

import java.util.List;
import org.apache.calcite.rel.RelRoot;

public class CreateTableAs extends CreateWithInput {
  public CreateTableAs(List<String> names, RelRoot input) {
    super(RelationType.TABLE, names, input);
  }

  @Override
  public <R> R accept(CalciteOperationVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
