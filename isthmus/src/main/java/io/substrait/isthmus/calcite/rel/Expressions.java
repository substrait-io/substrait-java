package io.substrait.isthmus.calcite.rel;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;

/**
 * Substrait specific Calcite relation that maps to {@link io.substrait.relation.VirtualTableScan}s
 * containing non-literal expressions.
 *
 * <p>Effectively a {@link org.apache.calcite.rel.core.Values} that allows full {@link RexNode}s in
 * tuples.
 */
public abstract class Expressions extends AbstractRelNode {

  public final List<List<RexNode>> tuples;

  protected Expressions(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType rowType,
      List<List<RexNode>> tuples) {
    super(cluster, traitSet);
    this.rowType = rowType;
    this.tuples = tuples;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    // Based off of Values#explainTerms
    RelDataType rowType = getRowType();
    RelWriter relWriter =
        super.explainTerms(pw)
            .itemIf("type", rowType, pw.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES)
            .itemIf("type", rowType.getFieldList(), pw.nest());
    if (pw.nest()) {
      pw.item("tuples", tuples);
    } else {
      pw.item(
          "tuples",
          tuples.stream()
              .map(
                  row ->
                      row.stream()
                          .map(RexNode::toString)
                          .collect(Collectors.joining(", ", "{ ", " }")))
              .collect(Collectors.joining(", ", "[", "]")));
    }
    return relWriter;
  }
}
