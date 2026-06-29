package io.substrait.dialect;

/** Write types for {@code WRITE} relations. */
public enum WriteType {
  /** A write to a named table. */
  NAMED_TABLE,
  /** A write to an extension-defined table. */
  EXTENSION_TABLE
}
