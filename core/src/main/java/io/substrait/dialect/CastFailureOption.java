package io.substrait.dialect;

/** Permissible failure options for {@code CAST} expressions. */
public enum CastFailureOption {
  RETURN_NULL,
  THROW_EXCEPTION
}
