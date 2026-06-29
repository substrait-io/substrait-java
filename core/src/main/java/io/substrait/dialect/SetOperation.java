package io.substrait.dialect;

/** Set operations for {@code SET} relations. */
public enum SetOperation {
  MINUS_PRIMARY,
  MINUS_PRIMARY_ALL,
  MINUS_MULTISET,
  INTERSECTION_PRIMARY,
  INTERSECTION_MULTISET,
  INTERSECTION_MULTISET_ALL,
  UNION_DISTINCT,
  UNION_ALL
}
