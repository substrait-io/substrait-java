package io.substrait.dialect;

/** The kinds of expressions a dialect can declare support for. */
public enum ExpressionKind {
  LITERAL,
  SELECTION,
  SCALAR_FUNCTION,
  WINDOW_FUNCTION,
  IF_THEN,
  SWITCH,
  SINGULAR_OR_LIST,
  MULTI_OR_LIST,
  CAST,
  SUBQUERY,
  NESTED,
  DYNAMIC_PARAMETER,
  EXECUTION_CONTEXT_VARIABLE
}
