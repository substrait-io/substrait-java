package io.substrait.isthmus.operation;

import java.util.List;

public abstract class DdlOperation extends CalciteOperation {
  public enum RelationType {
    TABLE,
    VIEW,
    MATERIALIZED_VIEW
    // TODO: , SCHEMA
  }

  public enum Operation {
    CREATE,
    DROP
  }

  public abstract List<String> getNames();

  public abstract RelationType getRelationType();

  public abstract Operation getOperation();

  @Override
  public <R> R accept(CalciteOperationVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
