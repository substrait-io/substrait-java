package io.substrait.dialect;

/** Subquery types for {@code SUBQUERY} expressions. */
public enum SubqueryType {
  SCALAR,
  IN_PREDICATE,
  SET_PREDICATE,
  SET_COMPARISON
}
