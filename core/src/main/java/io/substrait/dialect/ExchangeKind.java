package io.substrait.dialect;

/** Exchange kinds for {@code EXCHANGE} relations. */
public enum ExchangeKind {
  SCATTER_BY_FIELDS,
  SINGLE_TARGET,
  MULTI_TARGET,
  ROUND_ROBIN,
  BROADCAST
}
