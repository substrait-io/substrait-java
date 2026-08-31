package io.substrait.isthmus.calcite.rel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Litmus;

/**
 * A table of rows given as expressions, which is what a Substrait virtual table is and what Calcite
 * has no relation for: {@link org.apache.calcite.rel.core.Values} holds literals and nothing else.
 *
 * <p>Isthmus emits this for a virtual table whose rows do not all fit Values tuples, and recognises
 * it by type on the way back, so the relation comes back as the one it went in as. Expanding the
 * rows into stock Calcite relations -- a projection per row, unioned -- loses that: the shape a
 * planner leaves behind is a projection over an empty table, which is what converts back. A
 * consumer that needs the expansion can ask for it with {@link
 * io.substrait.isthmus.calcite.rel.rules.VirtualTableExpansionRule}, knowing it is one-way.
 *
 * <p>The rows are held at the row type's own field types, names included, so that the expansion has
 * nothing left to derive.
 */
public class VirtualTable extends AbstractRelNode {

  private final ImmutableList<List<RexNode>> rows;
  private final ImmutableSet<CorrelationId> variablesSet;

  /**
   * VirtualTable constructor.
   *
   * @param cluster the cluster this relation belongs to
   * @param traitSet the relation's traits
   * @param rowType the table's row type, carrying the schema's field names
   * @param variablesSet the correlation variables the rows resolve against
   * @param rows one list of values per row, each value at the type its column is declared at
   * @throws IllegalArgumentException if a row does not fit the row type
   */
  public VirtualTable(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType rowType,
      Set<CorrelationId> variablesSet,
      List<? extends List<RexNode>> rows) {
    super(cluster, traitSet);
    this.rowType = rowType;
    this.variablesSet = ImmutableSet.copyOf(variablesSet);
    ImmutableList.Builder<List<RexNode>> builder = ImmutableList.builder();
    for (List<RexNode> row : rows) {
      // Nothing else checks this: the deleted LogicalProject got it from RexUtil.compatibleTypes,
      // and AbstractRelNode.isValid succeeds unconditionally.
      if (row.size() != rowType.getFieldCount()) {
        throw new IllegalArgumentException(
            String.format(
                "A virtual table's row has %d values where its type declares %d columns: %s",
                row.size(), rowType.getFieldCount(), row));
      }
      if (!RexUtil.compatibleTypes(row, rowType, Litmus.IGNORE)) {
        throw new IllegalArgumentException(
            String.format(
                "A virtual table's row %s does not fit the type %s its columns are declared at",
                row, rowType.getFullTypeString()));
      }
      builder.add(ImmutableList.copyOf(row));
    }
    this.rows = builder.build();
  }

  /**
   * Creates a virtual table with no correlation variables, in the convention every relation this
   * conversion builds is in.
   *
   * @param cluster the cluster this relation belongs to
   * @param rowType the table's row type, carrying the schema's field names
   * @param rows one list of values per row
   * @return the virtual table
   */
  public static VirtualTable create(
      RelOptCluster cluster, RelDataType rowType, List<? extends List<RexNode>> rows) {
    return create(cluster, rowType, ImmutableSet.of(), rows);
  }

  /**
   * Creates a virtual table whose rows resolve against the given correlation variables.
   *
   * @param cluster the cluster this relation belongs to
   * @param rowType the table's row type, carrying the schema's field names
   * @param variablesSet the correlation variables the rows resolve against
   * @param rows one list of values per row
   * @return the virtual table
   */
  public static VirtualTable create(
      RelOptCluster cluster,
      RelDataType rowType,
      Set<CorrelationId> variablesSet,
      List<? extends List<RexNode>> rows) {
    return new VirtualTable(
        cluster, cluster.traitSetOf(Convention.NONE), rowType, variablesSet, rows);
  }

  /**
   * Returns the table's rows.
   *
   * @return one list of values per row
   */
  public List<List<RexNode>> getRows() {
    return rows;
  }

  /**
   * Returns the correlation variables the rows resolve against.
   *
   * <p>A row holding a {@link org.apache.calcite.rex.RexSubQuery} that binds an outer reference is
   * unreachable by {@code SubQueryRemoveRule}, whose operands are a projection, a filter and a
   * join, and by {@code RelDecorrelator}: a consumer's planner leaves it unexpanded, and the
   * variables it resolves against have to travel with the relation that holds it.
   *
   * <p>Only a consumer populates it. A conversion never does: an id is bound to a relation whose
   * fields the reference names, and a leaf with no inputs has none, so an outer reference in a row
   * belongs to the relation around the table the way one in a projection's expression does.
   *
   * @return the correlation variables
   */
  @Override
  public Set<CorrelationId> getVariablesSet() {
    return variablesSet;
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return rows.size();
  }

  /**
   * Returns the cost of this relation, which is the cost of what it expands into.
   *
   * <p>The inherited cost is a row count alone, which is cheaper than the projection per row the
   * expansion builds -- a cost-based planner would fire {@link
   * io.substrait.isthmus.calcite.rel.rules.VirtualTableExpansionRule} and then keep the unexpanded
   * relation it started from.
   *
   * @param planner the planner asking
   * @param mq the metadata query
   * @return the cost of this relation
   */
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    // Three relations per row -- an empty table, the projection over it, and the row's share of the
    // union -- each costing its own row and the values it computes.
    double relations = 3 * rows.size();
    return planner
        .getCostFactory()
        .makeCost(relations, relations * (rowType.getFieldCount() + 1), 0);
  }

  /**
   * Applies an expression rewrite to the rows.
   *
   * <p>Calcite rewrites a relation's expressions by handing it a shuttle, and a relation that does
   * not pass one on keeps its expressions out of everything built on that -- finding the subqueries
   * that bind outer references among them.
   *
   * <p>A rewrite that retypes a value retypes the column it stands in, so the row type is rebuilt
   * from the rewritten rows where they no longer carry the declared types. The names of a rebuilt
   * column are the expression's own below the top level, which is the most a relation can say
   * without the schema that named them.
   *
   * @param shuttle the rewrite to apply
   * @return this table with the rewritten rows, or itself where nothing changed
   */
  @Override
  public RelNode accept(RexShuttle shuttle) {
    List<List<RexNode>> rewritten = new ArrayList<>(rows.size());
    boolean changed = false;
    for (List<RexNode> row : rows) {
      List<RexNode> rewrittenRow = shuttle.apply(row);
      changed |= rewrittenRow != row;
      rewritten.add(rewrittenRow);
    }
    if (!changed) {
      return this;
    }
    RelDataType rewrittenType =
        rewritten.stream().allMatch(row -> RexUtil.compatibleTypes(row, rowType, Litmus.IGNORE))
            ? rowType
            : RexUtil.createStructType(
                getCluster().getTypeFactory(), rewritten.get(0), rowType.getFieldNames(), null);
    return new VirtualTable(getCluster(), getTraitSet(), rewrittenType, variablesSet, rewritten);
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
        .itemIf("type", rowType.getFieldList(), pw.nest())
        .itemIf("variablesSet", variablesSet, !variablesSet.isEmpty())
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
    return new VirtualTable(getCluster(), traitSet, rowType, variablesSet, rows);
  }
}
