package io.substrait.dialect;

/** The kinds of expressions a dialect can declare support for. */
public enum ExpressionKind {
  /** A literal value expression. */
  LITERAL,
  /** A field selection (reference) expression. */
  SELECTION,
  /** A scalar function call expression. */
  SCALAR_FUNCTION,
  /** A window function call expression. */
  WINDOW_FUNCTION,
  /** An if-then conditional expression. */
  IF_THEN,
  /** A switch expression. */
  SWITCH,
  /** A singular {@code OR}-list (IN) expression. */
  SINGULAR_OR_LIST,
  /** A multi-valued {@code OR}-list expression. */
  MULTI_OR_LIST,
  /** A cast expression. */
  CAST,
  /** A subquery expression. */
  SUBQUERY,
  /** A nested (struct/list/map) constructor expression. */
  NESTED,
  /** A dynamic parameter expression. */
  DYNAMIC_PARAMETER,
  /** An execution-context variable expression. */
  EXECUTION_CONTEXT_VARIABLE
}
