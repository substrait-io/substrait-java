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

/**
 * Visitor for {@code Rel} nodes.
 *
 * @param <O> result type returned by each visit
 * @param <C> visitation context type
 * @param <E> exception type that visit methods may throw
 */
public interface RelVisitor<O, C extends VisitationContext, E extends Exception> {

  /**
   * Visit an aggregate relation.
   *
   * @param aggregate the aggregate node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(Aggregate aggregate, C context) throws E;

  /**
   * Visit a fetch (limit/offset) relation.
   *
   * @param fetch the fetch node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(Fetch fetch, C context) throws E;

  /**
   * Visit a filter relation.
   *
   * @param filter the filter node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(Filter filter, C context) throws E;

  /**
   * Visit a logical join relation.
   *
   * @param join the join node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(Join join, C context) throws E;

  /**
   * Visit a set operation relation (e.g., UNION/INTERSECT).
   *
   * @param set the set node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(Set set, C context) throws E;

  /**
   * Visit a named scan relation.
   *
   * @param namedScan the named scan node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(NamedScan namedScan, C context) throws E;

  /**
   * Visit a local files scan relation.
   *
   * @param localFiles the local files node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(LocalFiles localFiles, C context) throws E;

  /**
   * Visit a project relation.
   *
   * @param project the project node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(Project project, C context) throws E;

  /**
   * Visit an expand relation (e.g., generators).
   *
   * @param expand the expand node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(Expand expand, C context) throws E;

  /**
   * Visit a sort relation.
   *
   * @param sort the sort node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(Sort sort, C context) throws E;

  /**
   * Visit a cross product relation.
   *
   * @param cross the cross node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(Cross cross, C context) throws E;

  /**
   * Visit a virtual table scan relation.
   *
   * @param virtualTableScan the virtual table scan node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(VirtualTableScan virtualTableScan, C context) throws E;

  /**
   * Visit an extension leaf relation.
   *
   * @param extensionLeaf the extension leaf node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(ExtensionLeaf extensionLeaf, C context) throws E;

  /**
   * Visit an extension single-input relation.
   *
   * @param extensionSingle the extension single node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(ExtensionSingle extensionSingle, C context) throws E;

  /**
   * Visit an extension multi-input relation.
   *
   * @param extensionMulti the extension multi node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(ExtensionMulti extensionMulti, C context) throws E;

  /**
   * Visit an extension table relation.
   *
   * @param extensionTable the extension table node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(ExtensionTable extensionTable, C context) throws E;

  /**
   * Visit a physical hash join relation.
   *
   * @param hashJoin the hash join node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(HashJoin hashJoin, C context) throws E;

  /**
   * Visit a physical merge join relation.
   *
   * @param mergeJoin the merge join node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(MergeJoin mergeJoin, C context) throws E;

  /**
   * Visit a physical nested loop join relation.
   *
   * @param nestedLoopJoin the nested loop join node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(NestedLoopJoin nestedLoopJoin, C context) throws E;

  /**
   * Visit a consistent partition window relation.
   *
   * @param consistentPartitionWindow the window node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(ConsistentPartitionWindow consistentPartitionWindow, C context) throws E;

  /**
   * Visit a named write relation.
   *
   * @param write the named write node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(NamedWrite write, C context) throws E;

  /**
   * Visit an extension write relation.
   *
   * @param write the extension write node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(ExtensionWrite write, C context) throws E;

  /**
   * Visit a named DDL relation.
   *
   * @param ddl the named DDL node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(NamedDdl ddl, C context) throws E;

  /**
   * Visit an extension DDL relation.
   *
   * @param ddl the extension DDL node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(ExtensionDdl ddl, C context) throws E;

  /**
   * Visit a named update relation.
   *
   * @param update the named update node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(NamedUpdate update, C context) throws E;

  /**
   * Visit a scatter exchange relation.
   *
   * @param exchange the scatter exchange node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(ScatterExchange exchange, C context) throws E;

  /**
   * Visit a single-bucket exchange relation.
   *
   * @param exchange the single-bucket exchange node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(SingleBucketExchange exchange, C context) throws E;

  /**
   * Visit a multi-bucket exchange relation.
   *
   * @param exchange the multi-bucket exchange node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(MultiBucketExchange exchange, C context) throws E;

  /**
   * Visit a round-robin exchange relation.
   *
   * @param exchange the round-robin exchange node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(RoundRobinExchange exchange, C context) throws E;

  /**
   * Visit a broadcast exchange relation.
   *
   * @param exchange the broadcast exchange node
   * @param context visitation context
   * @return visit result
   * @throws E on visit failure
   */
  O visit(BroadcastExchange exchange, C context) throws E;
}
