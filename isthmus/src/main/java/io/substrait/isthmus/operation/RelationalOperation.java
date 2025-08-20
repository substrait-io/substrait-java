package io.substrait.isthmus.operation;

import org.apache.calcite.rel.RelRoot;

public class RelationalOperation extends CalciteOperation {
  private final RelRoot relRoot;

  public RelationalOperation(RelRoot relRoot) {
    this.relRoot = relRoot;
  }

  public RelRoot getRelRoot() {
    return relRoot;
  }

  @Override
  public <R> R accept(CalciteOperationVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
