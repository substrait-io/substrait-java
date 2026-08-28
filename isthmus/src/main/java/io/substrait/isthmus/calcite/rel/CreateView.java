package io.substrait.isthmus.calcite.rel;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;

/** Represents a CREATE VIEW DDL operation in Calcite's relational algebra. */
public class CreateView extends SingleRel {
  private final List<String> viewName;
  private final RelDataType viewSchema;

  private CreateView(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<String> viewName,
      RelDataType viewSchema,
      RelNode input) {
    super(cluster, traitSet, input);
    this.viewName = viewName;
    this.viewSchema = DdlSchemas.requireFilledBy(viewSchema, input, "view");
  }

  /**
   * CreateView Constructor, taking the row type of the input as the schema of the view to create.
   *
   * @param viewName view name components
   * @param input RelNode input
   */
  public CreateView(List<String> viewName, RelNode input) {
    this(input.getCluster(), input.getTraitSet(), viewName, input.getRowType(), input);
  }

  /**
   * CreateView Constructor.
   *
   * @param viewName view name components
   * @param viewSchema the schema of the view to create, which the definition fills but need not
   *     name the same way
   * @param input RelNode input
   */
  public CreateView(List<String> viewName, RelDataType viewSchema, RelNode input) {
    this(input.getCluster(), input.getTraitSet(), viewName, viewSchema, input);
  }

  /**
   * Returns the schema of the view this node creates, which is what it produces: the input fills
   * it, and its own columns are the ones the statement declares.
   *
   * @return the schema of the view this node creates
   */
  @Override
  protected RelDataType deriveRowType() {
    return viewSchema;
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
        .item("viewName", getViewName())
        .item("viewSchema", getViewSchema().getFullTypeString());
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
          "CreateView requires exactly one input, but got " + inputs.size());
    }
    return new CreateView(getCluster(), traitSet, viewName, viewSchema, inputs.get(0));
  }

  /**
   * Returns the fully qualified view name parts.
   *
   * @return view name components (e.g., [schema, view])
   */
  public List<String> getViewName() {
    return viewName;
  }

  /**
   * Returns the schema of the view to create: the one the statement declares where this node was
   * built with it, and the row type of the definition otherwise. A declared schema holds the same
   * columns as the definition, under the names -- and the types -- the statement gives them.
   *
   * @return the schema of the view this node creates
   */
  public RelDataType getViewSchema() {
    return viewSchema;
  }
}
