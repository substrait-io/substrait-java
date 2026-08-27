package io.substrait.isthmus.calcite.rel;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;

/** Represents a CREATE TABLE DDL operation in Calcite's relational algebra. */
public class CreateTable extends SingleRel {

  private final List<String> tableName;
  private final RelDataType tableSchema;

  private CreateTable(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<String> tableName,
      RelDataType tableSchema,
      RelNode input) {
    super(cluster, traitSet, input);
    this.tableName = tableName;
    this.tableSchema = DdlSchemas.requireFilledBy(tableSchema, input, "table");
  }

  /**
   * CreateTable Constructor, taking the row type of the input as the schema of the table to create.
   *
   * @param tableName tablename components
   * @param input RelNode input
   */
  public CreateTable(List<String> tableName, RelNode input) {
    this(input.getCluster(), input.getTraitSet(), tableName, input.getRowType(), input);
  }

  /**
   * CreateTable Constructor.
   *
   * @param tableName tablename components
   * @param tableSchema the schema of the table to create, which the input fills but need not name
   *     the same way
   * @param input RelNode input
   */
  public CreateTable(List<String> tableName, RelDataType tableSchema, RelNode input) {
    this(input.getCluster(), input.getTraitSet(), tableName, tableSchema, input);
  }

  /**
   * Returns the schema of the table this node creates, which is what it produces: the input fills
   * it, and its own columns are the ones the statement declares.
   *
   * @return the schema of the table this node creates
   */
  @Override
  protected RelDataType deriveRowType() {
    return tableSchema;
  }

  /**
   * Explains the node terms for plan output.
   *
   * @param pw plan writer
   * @return the plan writer with this node's fields added
   */
  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("tableName", getTableName())
        .item("tableSchema", getTableSchema().getFullTypeString());
  }

  /**
   * Copies this node with the given traits and input.
   *
   * @param traitSet the RelTraitSet
   * @param inputs List of RelNodes
   * @return a copy of this node with the given input
   * @throws IllegalArgumentException if given anything but exactly one input
   */
  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    if (inputs.size() != 1) {
      throw new IllegalArgumentException(
          "CreateTable requires exactly one input, but got " + inputs.size());
    }
    return new CreateTable(getCluster(), traitSet, tableName, tableSchema, inputs.get(0));
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
   * Returns the schema of the table to create: the one the statement declares where this node was
   * built with it, and the row type of the input otherwise. A declared schema holds the same
   * columns as the input, under the names -- and the types -- the statement gives them.
   *
   * @return the schema of the table this node creates
   */
  public RelDataType getTableSchema() {
    return tableSchema;
  }
}
