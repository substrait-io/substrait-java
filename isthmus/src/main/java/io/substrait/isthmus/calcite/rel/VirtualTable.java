package io.substrait.isthmus.calcite.rel;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
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

  /**
   * VirtualTable constructor.
   *
   * @param cluster the cluster this relation belongs to
   * @param traitSet the relation's traits
   * @param rowType the table's row type, carrying the schema's field names
   * @param rows one list of values per row, each value at the type its column is declared at
   * @throws IllegalArgumentException if a row has more or fewer values than the type has columns
   */
  public VirtualTable(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType rowType,
      List<? extends List<RexNode>> rows) {
    super(cluster, traitSet);
    this.rowType = rowType;
    ImmutableList.Builder<List<RexNode>> builder = ImmutableList.builder();
    for (List<RexNode> row : rows) {
      // Nothing else checks the count: the deleted LogicalProject got it from
      // RexUtil.compatibleTypes, and AbstractRelNode.isValid succeeds unconditionally. The value
      // types are not checked at all: the conversion gives every value the type its column is
      // declared at before building the table, so a check here would only bind a direct caller.
      if (row.size() != rowType.getFieldCount()) {
        throw new IllegalArgumentException(
            String.format(
                "A virtual table's row has %d values where its type declares %d columns: %s",
                row.size(), rowType.getFieldCount(), row));
      }
      builder.add(ImmutableList.copyOf(row));
    }
    this.rows = builder.build();
  }

  /**
   * Creates a virtual table in the convention every relation this conversion builds is in.
   *
   * @param cluster the cluster this relation belongs to
   * @param rowType the table's row type, carrying the schema's field names
   * @param rows one list of values per row
   * @return the virtual table
   */
  public static VirtualTable create(
      RelOptCluster cluster, RelDataType rowType, List<? extends List<RexNode>> rows) {
    return new VirtualTable(cluster, cluster.traitSetOf(Convention.NONE), rowType, rows);
  }

  /**
   * Returns the table's rows.
   *
   * @return one list of values per row
   */
  public List<List<RexNode>> getRows() {
    return rows;
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
    return new VirtualTable(getCluster(), getTraitSet(), rebuiltRowType(rewritten), rewritten);
  }

  /**
   * The row type the rewritten rows fit.
   *
   * <p>The declared one where every row still carries it, so that a substitution keeping the types
   * keeps the schema's names, nested ones included. Otherwise the least restrictive type each
   * column takes across all of the rows: a shuttle that retypes a value in one row need not retype
   * the same column in the next, and a type taken from one row alone need not fit the others.
   *
   * @param rewritten the rows after the rewrite
   * @return the row type to build the rewritten table at
   */
  private RelDataType rebuiltRowType(List<List<RexNode>> rewritten) {
    if (rewritten.stream().allMatch(row -> RexUtil.compatibleTypes(row, rowType, Litmus.IGNORE))) {
      return rowType;
    }
    RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
    List<RelDataType> columnTypes = new ArrayList<>(rowType.getFieldCount());
    for (int column = 0; column < rowType.getFieldCount(); column++) {
      List<RelDataType> valueTypes = new ArrayList<>(rewritten.size());
      for (List<RexNode> row : rewritten) {
        valueTypes.add(row.get(column).getType());
      }
      RelDataType columnType = typeFactory.leastRestrictive(valueTypes);
      // Nothing in Calcite's type system unifies every pair -- a rewrite that leaves two rows with
      // no common type keeps the declared one, and the constructor is left to report the row.
      columnTypes.add(
          columnType == null ? rowType.getFieldList().get(column).getType() : columnType);
    }
    return typeFactory.createStructType(columnTypes, rowType.getFieldNames());
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
