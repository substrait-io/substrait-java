package io.substrait.isthmus.calcite.rel;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;

public class CreateTable extends SingleRel {

  private final List<String> tableName;

  private CreateTable(
      RelOptCluster cluster, RelTraitSet traitSet, List<String> tableName, RelNode input) {
    super(cluster, traitSet, input);
    this.tableName = tableName;
  }

  public CreateTable(List<String> tableName, RelNode input) {
    this(input.getCluster(), input.getTraitSet(), tableName, input);
  }

  /**
   * Explains the node terms for plan output.
   *
   * @param pw plan writer
   * @return the plan writer with this node's fields added
   */
  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("tableName", getTableName());
  }

  /**
   * Returns the inputs to this node (single input).
   *
   * @return a list containing the input relation
   */
  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.size() == 1;
    return new CreateTable(getCluster(), traitSet, tableName, inputs.get(0));
  }

  /**
   * Returns the fully qualified table name parts.
   *
   * @return table name components (e.g., [schema, table])
   */
  public List<String> getTableName() {
    return tableName;
  }
}
