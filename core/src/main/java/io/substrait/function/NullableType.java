package io.substrait.function;

/** A type that may carry nullability information. */
public interface NullableType {
  /**
   * Returns whether this type is nullable.
   *
   * @return {@code true} if the type accepts null values, {@code false} otherwise
   */
  boolean nullable();
}
