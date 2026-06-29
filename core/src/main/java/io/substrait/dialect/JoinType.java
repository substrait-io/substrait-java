package io.substrait.dialect;

/** Join types for join relations. */
public enum JoinType {
  INNER,
  OUTER,
  LEFT,
  RIGHT,
  LEFT_SEMI,
  RIGHT_SEMI,
  LEFT_ANTI,
  RIGHT_ANTI,
  LEFT_SINGLE,
  RIGHT_SINGLE,
  LEFT_MARK,
  RIGHT_MARK
}
