package io.substrait.dialect;

/** Read types for {@code READ} relations. */
public enum ReadType {
  /** A read from an inline virtual table. */
  VIRTUAL_TABLE,
  /** A read from local files. */
  LOCAL_FILES,
  /** A read from a named table. */
  NAMED_TABLE,
  /** A read from an extension-defined table. */
  EXTENSION_TABLE,
  /** A read from an Iceberg table. */
  ICEBERG_TABLE
}
