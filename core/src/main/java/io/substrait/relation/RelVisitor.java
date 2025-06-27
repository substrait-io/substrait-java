package io.substrait.relation;

import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.NestedLoopJoin;
import io.substrait.util.VisitationContext;

public interface RelVisitor<O, C extends VisitationContext, E extends Exception> {
  O visit(Aggregate aggregate, C context) throws E;

  O visit(EmptyScan emptyScan, C context) throws E;

  O visit(Fetch fetch, C context) throws E;

  O visit(Filter filter, C context) throws E;

  O visit(Join join, C context) throws E;

  O visit(Set set, C context) throws E;

  O visit(NamedScan namedScan, C context) throws E;

  O visit(LocalFiles localFiles, C context) throws E;

  O visit(Project project, C context) throws E;

  O visit(Expand expand, C context) throws E;

  O visit(Sort sort, C context) throws E;

  O visit(Cross cross, C context) throws E;

  O visit(VirtualTableScan virtualTableScan, C context) throws E;

  O visit(ExtensionLeaf extensionLeaf, C context) throws E;

  O visit(ExtensionSingle extensionSingle, C context) throws E;

  O visit(ExtensionMulti extensionMulti, C context) throws E;

  O visit(ExtensionTable extensionTable, C context) throws E;

  O visit(HashJoin hashJoin, C context) throws E;

  O visit(MergeJoin mergeJoin, C context) throws E;

  O visit(NestedLoopJoin nestedLoopJoin, C context) throws E;

  O visit(ConsistentPartitionWindow consistentPartitionWindow, C context) throws E;

  O visit(NamedWrite write, C context) throws E;

  O visit(ExtensionWrite write, C context) throws E;

  O visit(NamedDdl ddl, C context) throws E;

  O visit(ExtensionDdl ddl, C context) throws E;

  O visit(NamedUpdate update, C context) throws E;
}
