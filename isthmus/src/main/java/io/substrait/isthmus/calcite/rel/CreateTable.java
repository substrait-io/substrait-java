package io.substrait.isthmus.calcite.rel;

import java.util.List;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Synthetic relational node representing a {@code CREATE TABLE AS SELECT} operation.
 *
 * <p>Holds the target table name and the input relation whose schema/data will be used to create
 * the table.
 */
public class CreateTable extends AbstractRelNode {

  private final List<String> tableName;
  private final RelNode input;

  /**
   * Constructs a {@code CreateTable} node with the given table name and input relation.
   *
   * @param tableName fully qualified table name parts (e.g., schema and table)
   * @param input input relational node supplying schema and data
   */
  public CreateTable(List<String> tableName, RelNode input) {
    super(input.getCluster(), input.getTraitSet());

    this.tableName = tableName;
    this.input = input;
  }

  /**
   * Derives the row type from the input relation.
   *
   * @return the input {@link RelNode}'s row type
   */
  @Override
  protected RelDataType deriveRowType() {
    return input.getRowType();
  }

  /**
   * Explains the node terms for plan output.
   *
   * @param pw plan writer
   * @return the plan writer with this node's fields added
   */
  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).input("input", getInput()).item("tableName", getTableName());
  }

  /**
   * Returns the inputs to this node (single input).
   *
   * @return a list containing the input relation
   */
  @Override
  public List<RelNode> getInputs() {
    return List.of(input);
  }

  /**
   * Returns the fully qualified table name parts.
   *
   * @return table name components (e.g., [schema, table])
   */
  public List<String> getTableName() {
    return tableName;
  }

  /**
   * Returns the input relation for the CTAS operation.
   *
   * @return input {@link RelNode}
   */
  public RelNode getInput() {
    return input;
  }
}
