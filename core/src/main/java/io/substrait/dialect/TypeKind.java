package io.substrait.dialect;

/** The kinds of types a dialect can declare support for. */
public enum TypeKind {
  /** The boolean type. */
  BOOL,
  /** The 8-bit signed integer type. */
  I8,
  /** The 16-bit signed integer type. */
  I16,
  /** The 32-bit signed integer type. */
  I32,
  /** The 64-bit signed integer type. */
  I64,
  /** The 32-bit floating-point type. */
  FP32,
  /** The 64-bit floating-point type. */
  FP64,
  /** The variable-length binary type. */
  BINARY,
  /** The fixed-length binary type. */
  FIXED_BINARY,
  /** The variable-length string type. */
  STRING,
  /** The variable-length character type with a maximum length. */
  VARCHAR,
  /** The fixed-length character type. */
  FIXED_CHAR,
  /** The time-of-day type with configurable precision. */
  PRECISION_TIME,
  /** The timestamp type with configurable precision. */
  PRECISION_TIMESTAMP,
  /** The timezone-aware timestamp type with configurable precision. */
  PRECISION_TIMESTAMP_TZ,
  /** The calendar date type. */
  DATE,
  /** The time-of-day type. */
  TIME,
  /** The compound interval type. */
  INTERVAL_COMPOUND,
  /** The day-based interval type. */
  INTERVAL_DAY,
  /** The year-based interval type. */
  INTERVAL_YEAR,
  /** The UUID type. */
  UUID,
  /** The fixed-point decimal type. */
  DECIMAL,
  /** The struct (record) type. */
  STRUCT,
  /** The list type. */
  LIST,
  /** The map type. */
  MAP,
  /** A user-defined extension type. */
  USER_DEFINED
}
