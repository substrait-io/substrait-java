package io.substrait.isthmus;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil.SubQueryCollector;

/**
 * Resolve correlated variables and compute a depth map for {@link RexFieldAccess}.
 *
 * <p>Traverses a {@link RelNode} tree and:
 *
 * <ul>
 *   <li>Tracks nesting depth of {@link CorrelationId}s across filters, projects, subqueries, and
 *       correlates
 *   <li>Computes "steps out" for each {@link RexFieldAccess} referencing a {@link
 *       RexCorrelVariable}
 * </ul>
 *
 * See OuterReferenceResolver.md for details on how the depth map is computed.
 */
public class OuterReferenceResolver extends RelNodeVisitor<RelNode, RuntimeException> {

  private final Map<CorrelationId, Integer> nestedDepth;
  private final Map<RexFieldAccess, Integer> fieldAccessDepthMap;

  private final RexVisitor rexVisitor = new RexVisitor(this);

  /** Creates a new resolver with empty depth tracking maps. */
  public OuterReferenceResolver() {
    nestedDepth = new HashMap<>();
    fieldAccessDepthMap = new IdentityHashMap<>();
  }

  /**
   * Applies the resolver to a {@link RelNode} tree, computing the depth map.
   *
   * @param r the root relational node
   * @return the same node after traversal
   * @throws RuntimeException if the visitor encounters an unrecoverable condition
   */
  public Map<RexFieldAccess, Integer> apply(RelNode r) {
    reverseAccept(r);
    return fieldAccessDepthMap;
  }

  /**
   * Visits a {@link Filter}, registering any correlation variables and visiting its condition.
   *
   * @param filter the filter node
   * @return the result of {@link RelNodeVisitor#visit(Filter)}
   * @throws RuntimeException if traversal fails
   */
  @Override
  public RelNode visit(Filter filter) throws RuntimeException {
    for (CorrelationId id : filter.getVariablesSet()) {
      nestedDepth.putIfAbsent(id, 0);
    }
    filter.getCondition().accept(rexVisitor);
    return super.visit(filter);
  }

  /**
   * Visits a {@link Correlate}, handling correlation depth for both sides.
   *
   * <p>Special case: the right side is a correlated subquery in the rel tree (not a REX), so we
   * manually adjust depth before/after visiting it.
   *
   * @param correlate the correlate (correlated join) node
   * @return the correlate node
   * @throws RuntimeException if traversal fails
   */
  @Override
  public RelNode visit(Correlate correlate) throws RuntimeException {
    for (CorrelationId id : correlate.getVariablesSet()) {
      nestedDepth.putIfAbsent(id, 0);
    }

    apply(correlate.getLeft());

    // Correlated join is a special case. The right-rel is a correlated sub-query but not a REX. So,
    // the RexVisitor cannot be applied to it to correctly compute the depth map. Hence, we need to
    // manually compute the depth map for the right-rel.
    nestedDepth.replaceAll((k, v) -> v + 1);

    apply(correlate.getRight()); // look inside sub-queries

    nestedDepth.replaceAll((k, v) -> v - 1);

    return correlate;
  }

  /**
   * Visits a generic {@link RelNode}, applying traversal to all inputs.
   *
   * @param other the node to visit
   * @return the node
   * @throws RuntimeException if traversal fails
   */
  @Override
  public RelNode visitOther(RelNode other) throws RuntimeException {
    for (RelNode child : other.getInputs()) {
      apply(child);
    }
    return other;
  }

  /**
   * Visits a {@link Project}, registering correlation variables and visiting any subqueries within
   * its expressions.
   *
   * @param project the project node
   * @return the result of {@link RelNodeVisitor#visit(Project)}
   * @throws RuntimeException if traversal fails
   */
  @Override
  public RelNode visit(Project project) throws RuntimeException {
    for (CorrelationId id : project.getVariablesSet()) {
      nestedDepth.putIfAbsent(id, 0);
    }

    for (RexSubQuery subQuery : SubQueryCollector.collect(project)) {
      subQuery.accept(rexVisitor);
    }

    return super.visit(project);
  }

  /** Rex visitor used to track correlation depth within expressions and subqueries. */
  private static class RexVisitor extends RexShuttle {
    final OuterReferenceResolver referenceResolver;

    /**
     * Creates a new Rex visitor bound to the given reference resolver.
     *
     * @param referenceResolver the parent resolver maintaining depth maps
     */
    RexVisitor(OuterReferenceResolver referenceResolver) {
      this.referenceResolver = referenceResolver;
    }

    /**
     * Increments correlation depth when entering a subquery and decrements when exiting.
     *
     * @param subQuery the subquery expression
     * @return the same subquery
     */
    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
      referenceResolver.nestedDepth.replaceAll((k, v) -> v + 1);

      referenceResolver.apply(subQuery.rel); // look inside sub-queries

      referenceResolver.nestedDepth.replaceAll((k, v) -> v - 1);
      return subQuery;
    }

    /**
     * Records depth for {@link RexFieldAccess} referencing a {@link RexCorrelVariable}.
     *
     * @param fieldAccess the field access expression
     * @return the same field access
     */
    @Override
    public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
        CorrelationId id = ((RexCorrelVariable) fieldAccess.getReferenceExpr()).id;
        if (referenceResolver.nestedDepth.containsKey(id)) {
          referenceResolver.fieldAccessDepthMap.put(
              fieldAccess, referenceResolver.nestedDepth.get(id));
        }
      }
      return fieldAccess;
    }
  }
}
