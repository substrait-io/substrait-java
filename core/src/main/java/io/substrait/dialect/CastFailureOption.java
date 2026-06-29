package io.substrait.dialect;

/** Permissible failure options for {@code CAST} expressions. */
public enum CastFailureOption {
  /** Return null when a cast fails. */
  RETURN_NULL,
  /** Throw an exception when a cast fails. */
  THROW_EXCEPTION
}
