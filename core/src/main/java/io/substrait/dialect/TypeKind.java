package io.substrait.dialect;

/** The kinds of types a dialect can declare support for. */
public enum TypeKind {
  BOOL,
  I8,
  I16,
  I32,
  I64,
  FP32,
  FP64,
  BINARY,
  FIXED_BINARY,
  STRING,
  VARCHAR,
  FIXED_CHAR,
  PRECISION_TIME,
  PRECISION_TIMESTAMP,
  PRECISION_TIMESTAMP_TZ,
  DATE,
  TIME,
  INTERVAL_COMPOUND,
  INTERVAL_DAY,
  INTERVAL_YEAR,
  UUID,
  DECIMAL,
  STRUCT,
  LIST,
  MAP,
  USER_DEFINED
}
