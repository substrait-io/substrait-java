package io.substrait.relation;

import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.NestedLoopJoin;

public interface RelVisitor<OUTPUT, EXCEPTION extends Exception> {
  OUTPUT visit(Aggregate aggregate) throws EXCEPTION;

  OUTPUT visit(EmptyScan emptyScan) throws EXCEPTION;

  OUTPUT visit(Fetch fetch) throws EXCEPTION;

  OUTPUT visit(Filter filter) throws EXCEPTION;

  OUTPUT visit(Join join) throws EXCEPTION;

  OUTPUT visit(Set set) throws EXCEPTION;

  OUTPUT visit(NamedScan namedScan) throws EXCEPTION;

  OUTPUT visit(LocalFiles localFiles) throws EXCEPTION;

  OUTPUT visit(Project project) throws EXCEPTION;

  OUTPUT visit(Sort sort) throws EXCEPTION;

  OUTPUT visit(Cross cross) throws EXCEPTION;

  OUTPUT visit(VirtualTableScan virtualTableScan) throws EXCEPTION;

  OUTPUT visit(ExtensionLeaf extensionLeaf) throws EXCEPTION;

  OUTPUT visit(ExtensionSingle extensionSingle) throws EXCEPTION;

  OUTPUT visit(ExtensionMulti extensionMulti) throws EXCEPTION;

  OUTPUT visit(ExtensionTable extensionTable) throws EXCEPTION;

  OUTPUT visit(HashJoin hashJoin) throws EXCEPTION;

  OUTPUT visit(MergeJoin mergeJoin) throws EXCEPTION;

  OUTPUT visit(NestedLoopJoin nestedLoopJoin) throws EXCEPTION;

  OUTPUT visit(ConsistentPartitionWindow consistentPartitionWindow) throws EXCEPTION;

  OUTPUT visit(MatchRecognize matchRecognize) throws EXCEPTION;
}
