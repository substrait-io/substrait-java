package io.substrait.isthmus.operation;

public interface CalciteOperationVisitor<R> {
  R visit(CalciteOperation operation);

  R visit(RelationalOperation relationalOperation);

  R visit(DdlOperation ddlOperation);

  R visit(CreateWithInput createWithInput);

  R visit(Create create);

  R visit(CreateTableAs createTableAs);

  R visit(CreateView createView);
}
