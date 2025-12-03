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

/** Resolve correlated variable and get Depth map for RexFieldAccess */
// See OuterReferenceResolver.md for explanation how the Depth map is computed.
public class OuterReferenceResolver extends RelNodeVisitor<RelNode, RuntimeException> {

  private final Map<CorrelationId, Integer> nestedDepth;
  private final Map<RexFieldAccess, Integer> fieldAccessDepthMap;

  private final RexVisitor rexVisitor = new RexVisitor(this);

  public OuterReferenceResolver() {
    nestedDepth = new HashMap<>();
    fieldAccessDepthMap = new IdentityHashMap<>();
  }

  public int getStepsOut(final RexFieldAccess fieldAccess) {
    return fieldAccessDepthMap.get(fieldAccess);
  }

  public RelNode apply(final RelNode r) {
    return reverseAccept(r);
  }

  public Map<RexFieldAccess, Integer> getFieldAccessDepthMap() {
    return fieldAccessDepthMap;
  }

  @Override
  public RelNode visit(final Filter filter) throws RuntimeException {
    for (final CorrelationId id : filter.getVariablesSet()) {
      nestedDepth.putIfAbsent(id, 0);
    }
    filter.getCondition().accept(rexVisitor);
    return super.visit(filter);
  }

  @Override
  public RelNode visit(final Correlate correlate) throws RuntimeException {
    for (final CorrelationId id : correlate.getVariablesSet()) {
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

  @Override
  public RelNode visitOther(final RelNode other) throws RuntimeException {
    for (final RelNode child : other.getInputs()) {
      apply(child);
    }
    return other;
  }

  @Override
  public RelNode visit(final Project project) throws RuntimeException {
    for (final CorrelationId id : project.getVariablesSet()) {
      nestedDepth.putIfAbsent(id, 0);
    }

    for (final RexSubQuery subQuery : SubQueryCollector.collect(project)) {
      subQuery.accept(rexVisitor);
    }

    return super.visit(project);
  }

  private static class RexVisitor extends RexShuttle {
    final OuterReferenceResolver referenceResolver;

    RexVisitor(final OuterReferenceResolver referenceResolver) {
      this.referenceResolver = referenceResolver;
    }

    @Override
    public RexNode visitSubQuery(final RexSubQuery subQuery) {
      referenceResolver.nestedDepth.replaceAll((k, v) -> v + 1);

      referenceResolver.apply(subQuery.rel); // look inside sub-queries

      referenceResolver.nestedDepth.replaceAll((k, v) -> v - 1);
      return subQuery;
    }

    @Override
    public RexNode visitFieldAccess(final RexFieldAccess fieldAccess) {
      if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
        final CorrelationId id = ((RexCorrelVariable) fieldAccess.getReferenceExpr()).id;
        if (referenceResolver.nestedDepth.containsKey(id)) {
          referenceResolver.fieldAccessDepthMap.put(
              fieldAccess, referenceResolver.nestedDepth.get(id));
        }
      }
      return fieldAccess;
    }
  }
}
