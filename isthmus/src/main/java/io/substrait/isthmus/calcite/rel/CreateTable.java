package io.substrait.isthmus.calcite.rel;

import java.util.List;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;

public class CreateTable extends AbstractRelNode {

  private final List<String> tableName;
  private final RelNode input;

  public CreateTable(final List<String> tableName, final RelNode input) {
    super(input.getCluster(), input.getTraitSet());

    this.tableName = tableName;
    this.input = input;
  }

  @Override
  protected RelDataType deriveRowType() {
    return input.getRowType();
  }

  @Override
  public RelWriter explainTerms(final RelWriter pw) {
    return super.explainTerms(pw).input("input", getInput()).item("tableName", getTableName());
  }

  @Override
  public List<RelNode> getInputs() {
    return List.of(input);
  }

  public List<String> getTableName() {
    return tableName;
  }

  public RelNode getInput() {
    return input;
  }
}
