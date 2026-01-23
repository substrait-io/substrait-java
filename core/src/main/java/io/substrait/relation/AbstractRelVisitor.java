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
  public O visit(Aggregate aggregate, C context) throws E {
    return visitFallback(aggregate, context);
  }

  @Override
  public O visit(Fetch fetch, C context) throws E {
    return visitFallback(fetch, context);
  }

  @Override
  public O visit(Filter filter, C context) throws E {
    return visitFallback(filter, context);
  }

  @Override
  public O visit(Join join, C context) throws E {
    return visitFallback(join, context);
  }

  @Override
  public O visit(Set set, C context) throws E {
    return visitFallback(set, context);
  }

  @Override
  public O visit(NamedScan namedScan, C context) throws E {
    return visitFallback(namedScan, context);
  }

  @Override
  public O visit(LocalFiles localFiles, C context) throws E {
    return visitFallback(localFiles, context);
  }

  @Override
  public O visit(Project project, C context) throws E {
    return visitFallback(project, context);
  }

  @Override
  public O visit(Expand expand, C context) throws E {
    return visitFallback(expand, context);
  }

  @Override
  public O visit(Sort sort, C context) throws E {
    return visitFallback(sort, context);
  }

  @Override
  public O visit(Cross cross, C context) throws E {
    return visitFallback(cross, context);
  }

  @Override
  public O visit(VirtualTableScan virtualTableScan, C context) throws E {
    return visitFallback(virtualTableScan, context);
  }

  @Override
  public O visit(ExtensionLeaf extensionLeaf, C context) throws E {
    return visitFallback(extensionLeaf, context);
  }

  @Override
  public O visit(ExtensionSingle extensionSingle, C context) throws E {
    return visitFallback(extensionSingle, context);
  }

  @Override
  public O visit(ExtensionMulti extensionMulti, C context) throws E {
    return visitFallback(extensionMulti, context);
  }

  @Override
  public O visit(ExtensionTable extensionTable, C context) throws E {
    return visitFallback(extensionTable, context);
  }

  @Override
  public O visit(HashJoin hashJoin, C context) throws E {
    return visitFallback(hashJoin, context);
  }

  @Override
  public O visit(MergeJoin mergeJoin, C context) throws E {
    return visitFallback(mergeJoin, context);
  }

  @Override
  public O visit(NestedLoopJoin nestedLoopJoin, C context) throws E {
    return visitFallback(nestedLoopJoin, context);
  }

  @Override
  public O visit(ConsistentPartitionWindow consistentPartitionWindow, C context) throws E {
    return visitFallback(consistentPartitionWindow, context);
  }

  @Override
  public O visit(NamedWrite write, C context) throws E {
    return visitFallback(write, context);
  }

  @Override
  public O visit(ExtensionWrite write, C context) throws E {
    return visitFallback(write, context);
  }

  @Override
  public O visit(NamedDdl ddl, C context) throws E {
    return visitFallback(ddl, context);
  }

  @Override
  public O visit(ExtensionDdl ddl, C context) throws E {
    return visitFallback(ddl, context);
  }

  @Override
  public O visit(NamedUpdate update, C context) throws E {
    return visitFallback(update, context);
  }

  @Override
  public O visit(ScatterExchange exchange, C context) throws E {
    return visitFallback(exchange, context);
  }

  @Override
  public O visit(SingleBucketExchange exchange, C context) throws E {
    return visitFallback(exchange, context);
  }

  @Override
  public O visit(MultiBucketExchange exchange, C context) throws E {
    return visitFallback(exchange, context);
  }

  @Override
  public O visit(BroadcastExchange exchange, C context) throws E {
    return visitFallback(exchange, context);
  }

  @Override
  public O visit(RoundRobinExchange exchange, C context) throws E {
    return visitFallback(exchange, context);
  }
}
