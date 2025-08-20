package io.substrait.isthmus.operation;

public class CalciteOperation {
  public <R> R accept(CalciteOperationVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
