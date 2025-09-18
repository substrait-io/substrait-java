package io.substrait.isthmus.calcite.rel;

import java.util.List;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;

public class CreateTable extends AbstractRelNode {

  private final List<String> tableName;
  private final RelRoot input;

  public CreateTable(List<String> tableName, RelRoot input) {
    super(input.rel.getCluster(), input.rel.getTraitSet());

    this.tableName = tableName;
    this.input = input;
  }

  @Override
  protected RelDataType deriveRowType() {
    return input.validatedRowType;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).input("input", getInput().rel).item("tableName", getTableName());
  }

  @Override
  public List<RelNode> getInputs() {
    return List.of(input.rel);
  }

  public List<String> getTableName() {
    return tableName;
  }

  public RelRoot getInput() {
    return input;
  }
}
