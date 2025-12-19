package io.substrait.isthmus.expression;

/**
 * Enum to define the INDEXING property on the date functions.
 *
 * <p>Controls if the number used for example in months is 0 or 1 based.
 */
public enum ExtractIndexing {
  ONE,
  ZERO
}
