package io.substrait.isthmus.operation;

import java.util.List;

public abstract class Create extends DdlOperation {
  private final List<String> names;
  private final RelationType relationType;

  public Create(RelationType relationType, List<String> names) {
    this.relationType = relationType;
    this.names = names;
  }

  @Override
  public List<String> getNames() {
    return names;
  }

  @Override
  public RelationType getRelationType() {
    return relationType;
  }

  @Override
  public Operation getOperation() {
    return Operation.CREATE;
  }

  @Override
  public <R> R accept(CalciteOperationVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
