package io.substrait.isthmus.operation;

public class CalciteOperationBasicVisitor<R> implements CalciteOperationVisitor<R> {
  @Override
  public R visit(CalciteOperation operation) {
    return null;
  }

  @Override
  public R visit(RelationalOperation relationalOperation) {
    return visit((CalciteOperation) relationalOperation);
  }

  @Override
  public R visit(DdlOperation ddlOperation) {
    return visit((CalciteOperation) ddlOperation);
  }

  @Override
  public R visit(CreateWithInput createWithInput) {
    return visit((Create) createWithInput);
  }

  @Override
  public R visit(Create create) {
    return visit((DdlOperation) create);
  }

  @Override
  public R visit(CreateTableAs createTableAs) {
    return visit((CreateWithInput) createTableAs);
  }

  @Override
  public R visit(CreateView createView) {
    return visit((CreateWithInput) createView);
  }
}
