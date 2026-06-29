package io.substrait.dialect;

/** Read types for {@code READ} relations. */
public enum ReadType {
  VIRTUAL_TABLE,
  LOCAL_FILES,
  NAMED_TABLE,
  EXTENSION_TABLE,
  ICEBERG_TABLE
}
