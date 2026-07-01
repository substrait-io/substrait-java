package io.substrait.dialect;

/** Set operations for {@code SET} relations. */
public enum SetOperation {
  /** The primary set difference (minus) operation. */
  MINUS_PRIMARY,
  /** The primary set difference (minus) operation retaining all rows. */
  MINUS_PRIMARY_ALL,
  /** The multiset difference (minus) operation. */
  MINUS_MULTISET,
  /** The primary set intersection operation. */
  INTERSECTION_PRIMARY,
  /** The multiset intersection operation. */
  INTERSECTION_MULTISET,
  /** The multiset intersection operation retaining all rows. */
  INTERSECTION_MULTISET_ALL,
  /** The distinct union operation. */
  UNION_DISTINCT,
  /** The union-all operation. */
  UNION_ALL
}
