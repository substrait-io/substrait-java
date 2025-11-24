package io.substrait.isthmus.calcite.rel;

import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

/** Subclass of {@link Expressions} not targeted at any particular engine or calling convention. */
public class LogicalExpressions extends Expressions {

  /**
   * Creates a LogicalExpression.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   *
   * @param cluster Cluster that this relational expression belongs to
   * @param rowType Row type for tuples produced by this relation
   * @param tuples 2-dimensional array of tuple values to be produced; outer list contains tuples;
   *     each inner list is one tuple; all tuples must be of same length, conforming to rowType
   */
  public LogicalExpressions(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType rowType,
      List<List<RexNode>> tuples) {
    super(cluster, traitSet, rowType, tuples);
  }

  /**
   * Creates a LogicalExpression
   *
   * @param cluster Cluster that this relational expression belongs to
   * @param rowType Row type for tuples produced by this relation
   * @param tuples 2-dimensional array of tuple values to be produced; outer list contains tuples;
   *     each inner list is one tuple; all tuples must be of same length, conforming to rowType
   */
  public static LogicalExpressions create(
      RelOptCluster cluster, final RelDataType rowType, final List<List<RexNode>> tuples) {
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalExpressions(cluster, traitSet, rowType, tuples);
  }

  @Override
  public LogicalExpressions copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new LogicalExpressions(getCluster(), traitSet, getRowType(), tuples);
  }
}
