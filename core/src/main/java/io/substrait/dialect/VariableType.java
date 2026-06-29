package io.substrait.dialect;

/** Variable types for {@code EXECUTION_CONTEXT_VARIABLE} expressions. */
public enum VariableType {
  /** The current timestamp variable. */
  CURRENT_TIMESTAMP,
  /** The current timezone variable. */
  CURRENT_TIMEZONE,
  /** The current date variable. */
  CURRENT_DATE
}
