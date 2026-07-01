package io.substrait.dialect;

/** Operable object types for {@code DDL} relations. */
public enum DdlWriteType {
  /** A named DDL object. */
  NAMED_OBJECT,
  /** An extension-defined DDL object. */
  EXTENSION_OBJECT
}
