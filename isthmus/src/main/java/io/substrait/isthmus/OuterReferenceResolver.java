package io.substrait.isthmus;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.*;

/** Resolve correlated variable and get Depth map for RexFieldAccess */
public class OuterReferenceResolver extends RelNodeVisitor<RelNode, RuntimeException> {

  private Map<CorrelationId, Integer> nestedDepth;
  private Map<RexFieldAccess, Integer> fieldAccessDepthMap;

  private RexVisitor rexVisitor = new RexVisitor(this);

  public OuterReferenceResolver() {
    nestedDepth = new HashMap<>();
    fieldAccessDepthMap = new IdentityHashMap<>();
  }

  public int getStepsOut(RexFieldAccess fieldAccess) {
    return fieldAccessDepthMap.get(fieldAccess);
  }

  public RelNode apply(RelNode r) {
    return reverseAccept(r);
  }

  public Map<RexFieldAccess, Integer> getFieldAccessDepthMap() {
    return fieldAccessDepthMap;
  }

  @Override
  public RelNode visit(LogicalFilter filter) throws RuntimeException {
    for (CorrelationId id : filter.getVariablesSet()) {
      if (!nestedDepth.containsKey(id)) {
        nestedDepth.put(id, 0);
      }
    }
    filter.getCondition().accept(rexVisitor);
    return super.visit(filter);
  }

  @Override // FIXME: join / project with expressions (possible contain subquery)
  public RelNode visitOther(RelNode other) throws RuntimeException {
    for (RelNode child : other.getInputs()) {
      apply(child);
    }
    return other;
  }

  private class RexVisitor extends RexShuttle {
    final OuterReferenceResolver referenceResolver;

    RexVisitor(OuterReferenceResolver referenceResolver) {
      this.referenceResolver = referenceResolver;
    }

    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
      for (Map.Entry<CorrelationId, Integer> entry : referenceResolver.nestedDepth.entrySet()) {
        referenceResolver.nestedDepth.put(entry.getKey(), entry.getValue() + 1);
      }

      referenceResolver.apply(subQuery.rel); // look inside sub-queries

      for (Map.Entry<CorrelationId, Integer> entry : referenceResolver.nestedDepth.entrySet()) {
        referenceResolver.nestedDepth.put(entry.getKey(), entry.getValue() - 1);
      }
      return subQuery;
    }

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
