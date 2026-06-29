package io.substrait.dialect;

/** Field types for {@code EXPAND} relations. */
public enum ExpandFieldType {
  /** A field whose value switches between expressions per expansion. */
  SWITCHING_FIELD,
  /** A field with a constant value across expansions. */
  CONSTANT_FIELD
}
