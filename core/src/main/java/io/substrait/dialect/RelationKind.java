package io.substrait.dialect;

/** The kinds of relations a dialect can declare support for. */
public enum RelationKind {
  /** The read relation. */
  READ,
  /** The filter relation. */
  FILTER,
  /** The fetch (limit/offset) relation. */
  FETCH,
  /** The aggregate relation. */
  AGGREGATE,
  /** The sort relation. */
  SORT,
  /** The join relation. */
  JOIN,
  /** The project relation. */
  PROJECT,
  /** The set-operation relation. */
  SET,
  /** The cross-product relation. */
  CROSS,
  /** The reference relation. */
  REFERENCE,
  /** The write relation. */
  WRITE,
  /** The DDL relation. */
  DDL,
  /** The update relation. */
  UPDATE,
  /** The hash join relation. */
  HASH_JOIN,
  /** The merge join relation. */
  MERGE_JOIN,
  /** The nested-loop join relation. */
  NESTED_LOOP_JOIN,
  /** The consistent partition window relation. */
  CONSISTENT_PARTITION_WINDOW,
  /** The exchange relation. */
  EXCHANGE,
  /** The expand relation. */
  EXPAND,
  /** The single-input extension relation. */
  EXTENSION_SINGLE,
  /** The multi-input extension relation. */
  EXTENSION_MULTI,
  /** The leaf extension relation. */
  EXTENSION_LEAF
}
