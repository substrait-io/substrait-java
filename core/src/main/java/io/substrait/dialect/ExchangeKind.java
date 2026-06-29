package io.substrait.dialect;

/** Exchange kinds for {@code EXCHANGE} relations. */
public enum ExchangeKind {
  /** Distribute rows across targets by hashing fields. */
  SCATTER_BY_FIELDS,
  /** Route all rows to a single target. */
  SINGLE_TARGET,
  /** Route rows to multiple targets. */
  MULTI_TARGET,
  /** Distribute rows across targets in round-robin order. */
  ROUND_ROBIN,
  /** Broadcast all rows to every target. */
  BROADCAST
}
