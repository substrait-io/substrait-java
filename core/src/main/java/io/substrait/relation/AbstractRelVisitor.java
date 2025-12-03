package io.substrait.relation;

import io.substrait.relation.physical.BroadcastExchange;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.MultiBucketExchange;
import io.substrait.relation.physical.NestedLoopJoin;
import io.substrait.relation.physical.RoundRobinExchange;
import io.substrait.relation.physical.ScatterExchange;
import io.substrait.relation.physical.SingleBucketExchange;
import io.substrait.util.VisitationContext;

public abstract class AbstractRelVisitor<O, C extends VisitationContext, E extends Exception>
    implements RelVisitor<O, C, E> {
  public abstract O visitFallback(Rel rel, C context);

  @Override
  public O visit(final Aggregate aggregate, final C context) throws E {
    return visitFallback(aggregate, context);
  }

  @Override
  public O visit(final EmptyScan emptyScan, final C context) throws E {
    return visitFallback(emptyScan, context);
  }

  @Override
  public O visit(final Fetch fetch, final C context) throws E {
    return visitFallback(fetch, context);
  }

  @Override
  public O visit(final Filter filter, final C context) throws E {
    return visitFallback(filter, context);
  }

  @Override
  public O visit(final Join join, final C context) throws E {
    return visitFallback(join, context);
  }

  @Override
  public O visit(final Set set, final C context) throws E {
    return visitFallback(set, context);
  }

  @Override
  public O visit(final NamedScan namedScan, final C context) throws E {
    return visitFallback(namedScan, context);
  }

  @Override
  public O visit(final LocalFiles localFiles, final C context) throws E {
    return visitFallback(localFiles, context);
  }

  @Override
  public O visit(final Project project, final C context) throws E {
    return visitFallback(project, context);
  }

  @Override
  public O visit(final Expand expand, final C context) throws E {
    return visitFallback(expand, context);
  }

  @Override
  public O visit(final Sort sort, final C context) throws E {
    return visitFallback(sort, context);
  }

  @Override
  public O visit(final Cross cross, final C context) throws E {
    return visitFallback(cross, context);
  }

  @Override
  public O visit(final VirtualTableScan virtualTableScan, final C context) throws E {
    return visitFallback(virtualTableScan, context);
  }

  @Override
  public O visit(final ExtensionLeaf extensionLeaf, final C context) throws E {
    return visitFallback(extensionLeaf, context);
  }

  @Override
  public O visit(final ExtensionSingle extensionSingle, final C context) throws E {
    return visitFallback(extensionSingle, context);
  }

  @Override
  public O visit(final ExtensionMulti extensionMulti, final C context) throws E {
    return visitFallback(extensionMulti, context);
  }

  @Override
  public O visit(final ExtensionTable extensionTable, final C context) throws E {
    return visitFallback(extensionTable, context);
  }

  @Override
  public O visit(final HashJoin hashJoin, final C context) throws E {
    return visitFallback(hashJoin, context);
  }

  @Override
  public O visit(final MergeJoin mergeJoin, final C context) throws E {
    return visitFallback(mergeJoin, context);
  }

  @Override
  public O visit(final NestedLoopJoin nestedLoopJoin, final C context) throws E {
    return visitFallback(nestedLoopJoin, context);
  }

  @Override
  public O visit(final ConsistentPartitionWindow consistentPartitionWindow, final C context)
      throws E {
    return visitFallback(consistentPartitionWindow, context);
  }

  @Override
  public O visit(final NamedWrite write, final C context) throws E {
    return visitFallback(write, context);
  }

  @Override
  public O visit(final ExtensionWrite write, final C context) throws E {
    return visitFallback(write, context);
  }

  @Override
  public O visit(final NamedDdl ddl, final C context) throws E {
    return visitFallback(ddl, context);
  }

  @Override
  public O visit(final ExtensionDdl ddl, final C context) throws E {
    return visitFallback(ddl, context);
  }

  @Override
  public O visit(final NamedUpdate update, final C context) throws E {
    return visitFallback(update, context);
  }

  @Override
  public O visit(final ScatterExchange exchange, final C context) throws E {
    return visitFallback(exchange, context);
  }

  @Override
  public O visit(final SingleBucketExchange exchange, final C context) throws E {
    return visitFallback(exchange, context);
  }

  @Override
  public O visit(final MultiBucketExchange exchange, final C context) throws E {
    return visitFallback(exchange, context);
  }

  @Override
  public O visit(final BroadcastExchange exchange, final C context) throws E {
    return visitFallback(exchange, context);
  }

  @Override
  public O visit(final RoundRobinExchange exchange, final C context) throws E {
    return visitFallback(exchange, context);
  }
}
