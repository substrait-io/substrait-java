package io.substrait.dialect;

/** The kinds of relations a dialect can declare support for. */
public enum RelationKind {
  READ,
  FILTER,
  FETCH,
  AGGREGATE,
  SORT,
  JOIN,
  PROJECT,
  SET,
  CROSS,
  REFERENCE,
  WRITE,
  DDL,
  UPDATE,
  HASH_JOIN,
  MERGE_JOIN,
  NESTED_LOOP_JOIN,
  CONSISTENT_PARTITION_WINDOW,
  EXCHANGE,
  EXPAND,
  EXTENSION_SINGLE,
  EXTENSION_MULTI,
  EXTENSION_LEAF
}
