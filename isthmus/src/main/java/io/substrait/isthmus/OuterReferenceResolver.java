package io.substrait.isthmus;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil.SubQueryCollector;

/**
 * Assigns id-based outer-reference anchors to a {@link RelNode} tree.
 *
 * <p>Calcite models correlation with {@link CorrelationId}s, each bound by a single relation (a
 * {@link Correlate}'s left input, or the input of a {@link Filter}/{@link Project} that declares
 * the id). This maps directly onto Substrait's id-based outer references: the bound relation
 * carries a {@code rel_anchor} ({@link io.substrait.relation.Rel#getRelAnchor()}) and every
 * correlated field reference carries the matching {@code rel_reference} ({@link
 * io.substrait.expression.FieldReference#outerReferenceRelReference()}).
 *
 * <p>This resolver walks the tree (including subquery relations) and produces:
 *
 * <ul>
 *   <li>{@link #anchorForCorrelationId(CorrelationId)} — the anchor a correlated field reference
 *       should emit;
 *   <li>{@link #anchorForTarget(RelNode)} — the anchor to stamp on a bound relation.
 * </ul>
 *
 * Anchors are plan-wide unique and assigned per bound relation, so multiple correlation ids sharing
 * a relation share its anchor.
 */
public class OuterReferenceResolver extends RelNodeVisitor<RelNode, RuntimeException> {

  private final Map<CorrelationId, Integer> anchorByCorrelationId = new HashMap<>();
  private final Map<RelNode, Integer> anchorByTarget = new IdentityHashMap<>();

  private final RexVisitor rexVisitor = new RexVisitor(this);

  private int nextAnchor = 1;

  /** Creates a new resolver with empty anchor maps. */
  public OuterReferenceResolver() {}

  /**
   * Walks a {@link RelNode} tree, assigning outer-reference anchors.
   *
   * @param root the root relational node
   * @return the same node after traversal
   */
  public RelNode resolve(RelNode root) {
    reverseAccept(root);
    return root;
  }

  /**
   * Returns the anchor a correlated field reference bound to the given correlation id must emit as
   * its {@code rel_reference}.
   *
   * @param id the correlation id
   * @return the anchor, or {@code null} if the id was not resolved
   */
  public Integer anchorForCorrelationId(CorrelationId id) {
    return anchorByCorrelationId.get(id);
  }

  /**
   * Returns the anchor to stamp on the given relation, if it is the binding point of any outer
   * reference.
   *
   * @param rel the relation
   * @return the anchor, or {@code null} if the relation binds no outer reference
   */
  public Integer anchorForTarget(RelNode rel) {
    return anchorByTarget.get(rel);
  }

  /** Binds {@code id} to {@code target}, allocating {@code target}'s anchor on first use. */
  private void bind(CorrelationId id, RelNode target) {
    int anchor = anchorByTarget.computeIfAbsent(target, t -> nextAnchor++);
    anchorByCorrelationId.put(id, anchor);
  }

  @Override
  public RelNode visit(Filter filter) throws RuntimeException {
    for (CorrelationId id : filter.getVariablesSet()) {
      bind(id, filter.getInput());
    }
    filter.getCondition().accept(rexVisitor);
    return visitOther(filter);
  }

  @Override
  public RelNode visit(Project project) throws RuntimeException {
    for (CorrelationId id : project.getVariablesSet()) {
      bind(id, project.getInput());
    }
    for (RexSubQuery subQuery : SubQueryCollector.collect(project)) {
      subQuery.accept(rexVisitor);
    }
    return visitOther(project);
  }

  @Override
  public RelNode visit(Correlate correlate) throws RuntimeException {
    bind(correlate.getCorrelationId(), correlate.getLeft());
    reverseAccept(correlate.getLeft());
    reverseAccept(correlate.getRight());
    return correlate;
  }

  @Override
  public RelNode visitOther(RelNode other) throws RuntimeException {
    for (RelNode child : other.getInputs()) {
      reverseAccept(child);
    }
    return other;
  }

  /** Rex visitor that descends into subquery relations to reach nested correlations. */
  private static class RexVisitor extends RexShuttle {
    final OuterReferenceResolver referenceResolver;

    RexVisitor(OuterReferenceResolver referenceResolver) {
      this.referenceResolver = referenceResolver;
    }

    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
      referenceResolver.reverseAccept(subQuery.rel);
      return subQuery;
    }
  }
}
