package io.substrait.dialect;

/** Join types for join relations. */
public enum JoinType {
  /** An inner join. */
  INNER,
  /** A full outer join. */
  OUTER,
  /** A left outer join. */
  LEFT,
  /** A right outer join. */
  RIGHT,
  /** A left semi join. */
  LEFT_SEMI,
  /** A right semi join. */
  RIGHT_SEMI,
  /** A left anti join. */
  LEFT_ANTI,
  /** A right anti join. */
  RIGHT_ANTI,
  /** A left single join. */
  LEFT_SINGLE,
  /** A right single join. */
  RIGHT_SINGLE,
  /** A left mark join. */
  LEFT_MARK,
  /** A right mark join. */
  RIGHT_MARK
}
