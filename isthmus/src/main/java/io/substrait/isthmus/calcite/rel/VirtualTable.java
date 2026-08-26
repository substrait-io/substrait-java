package io.substrait.isthmus.calcite.rel;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlExplainLevel;

/**
 * A table of rows given as expressions, which is what a Substrait virtual table is and what Calcite
 * has no relation for: {@link org.apache.calcite.rel.core.Values} holds literals and nothing else.
 *
 * <p>Isthmus emits this for a virtual table whose rows do not all fit Values tuples, and recognises
 * it by type on the way back, so the relation comes back as the one it went in as. Expanding the
 * rows into stock Calcite relations -- a projection per row, unioned -- loses that: the shape a
 * planner leaves behind is a projection over an empty table, which is what converts back. A
 * consumer that needs the expansion can ask for it with {@link
 * io.substrait.isthmus.calcite.rel.rules.VirtualTableExpansionRule}, knowing it is one-way; nothing
 * in isthmus runs that rule.
 *
 * <p>The rows are held at the row type's own field types, names included, so that the expansion has
 * nothing left to derive.
 */
public class VirtualTable extends AbstractRelNode {

  private final ImmutableList<ImmutableList<RexNode>> rows;

  /**
   * VirtualTable constructor.
   *
   * @param cluster the cluster this relation belongs to
   * @param traitSet the relation's traits
   * @param rowType the table's row type, carrying the schema's field names
   * @param rows one list of values per row, each value at the type its column is declared at
   */
  public VirtualTable(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType rowType,
      List<? extends List<RexNode>> rows) {
    super(cluster, traitSet);
    this.rowType = rowType;
    ImmutableList.Builder<ImmutableList<RexNode>> builder = ImmutableList.builder();
    for (List<RexNode> row : rows) {
      builder.add(ImmutableList.copyOf(row));
    }
    this.rows = builder.build();
  }

  /**
   * Returns the table's rows.
   *
   * @return one list of values per row
   */
  public List<? extends List<RexNode>> getRows() {
    return rows;
  }

  @Override
  protected RelDataType deriveRowType() {
    return rowType;
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return rows.size();
  }

  /**
   * Applies an expression rewrite to the rows.
   *
   * <p>Calcite rewrites a relation's expressions by handing it a shuttle, and a relation that does
   * not pass one on keeps its expressions out of everything built on that -- finding the subqueries
   * that bind outer references among them.
   *
   * @param shuttle the rewrite to apply
   * @return this table with the rewritten rows, or itself where nothing changed
   */
  @Override
  public RelNode accept(RexShuttle shuttle) {
    boolean changed = false;
    List<List<RexNode>> rewritten = new ArrayList<>(rows.size());
    for (List<RexNode> row : rows) {
      List<RexNode> rewrittenRow = new ArrayList<>(row.size());
      for (RexNode value : row) {
        RexNode visited = value.accept(shuttle);
        changed |= visited != value;
        rewrittenRow.add(visited);
      }
      rewritten.add(rewrittenRow);
    }
    return changed ? new VirtualTable(getCluster(), getTraitSet(), rowType, rewritten) : this;
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
        .itemIf("type", rowType, pw.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES)
        .item(
            "rows",
            rows.stream()
                .map(
                    row ->
                        row.stream()
                            .map(RexNode::toString)
                            .collect(Collectors.joining(", ", "{ ", " }")))
                .collect(Collectors.joining(", ", "[", "]")));
  }

  /**
   * Copies this node with the given traits.
   *
   * @param traitSet the RelTraitSet
   * @param inputs List of RelNodes, which has to be empty
   * @return a copy of this node
   * @throws IllegalArgumentException if given any input
   */
  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    if (!inputs.isEmpty()) {
      throw new IllegalArgumentException("VirtualTable takes no inputs, but got " + inputs.size());
    }
    return new VirtualTable(getCluster(), traitSet, rowType, rows);
  }
}
