package io.substrait.dialect;

/** When execution context variables are evaluated. */
public enum VariableEvaluationMode {
  /** The variable is evaluated once per plan. */
  PER_PLAN,
  /** The variable is evaluated once per record. */
  PER_RECORD
}
