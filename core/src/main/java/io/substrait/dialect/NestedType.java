package io.substrait.dialect;

/** Nested types for {@code NESTED} expressions. */
public enum NestedType {
  /** A struct constructor. */
  STRUCT,
  /** A list constructor. */
  LIST,
  /** A map constructor. */
  MAP
}
