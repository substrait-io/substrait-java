package io.substrait.dialect;

/** Subquery types for {@code SUBQUERY} expressions. */
public enum SubqueryType {
  /** A scalar subquery. */
  SCALAR,
  /** An {@code IN} predicate subquery. */
  IN_PREDICATE,
  /** A set predicate ({@code EXISTS}/{@code UNIQUE}) subquery. */
  SET_PREDICATE,
  /** A set comparison ({@code ANY}/{@code ALL}) subquery. */
  SET_COMPARISON
}
