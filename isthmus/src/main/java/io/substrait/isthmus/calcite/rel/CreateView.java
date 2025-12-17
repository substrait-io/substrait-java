package io.substrait.isthmus.calcite.rel;

import java.util.List;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Synthetic relational node representing a {@code CREATE VIEW AS SELECT} operation.
 *
 * <p>Stores the view name and the input relation that defines the view.
 */
public class CreateView extends AbstractRelNode {
  private final List<String> viewName;
  private final RelNode input;

  /**
   * Constructs a {@code CreateView} node with the given view name and input relation.
   *
   * @param viewName fully qualified view name parts (e.g., schema and view)
   * @param input input relational node defining the view
   */
  public CreateView(List<String> viewName, RelNode input) {
    super(input.getCluster(), input.getTraitSet());
    this.viewName = viewName;
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
    return super.explainTerms(pw).input("input", getInput()).item("viewName", getViewName());
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
   * Returns the fully qualified view name parts.
   *
   * @return view name components (e.g., [schema, view])
   */
  public List<String> getViewName() {
    return viewName;
  }

  /**
   * Returns the input relation for the view definition.
   *
   * @return input {@link RelNode}
   */
  public RelNode getInput() {
    return input;
  }
}
