package io.substrait.dialect;

/** When execution context variables are evaluated. */
public enum VariableEvaluationMode {
  PER_PLAN,
  PER_RECORD
}
