package io.substrait.function;

/**
 * Factory for types whose parameters may themselves be expressions, used when building
 * parameterized types and type expressions.
 *
 * @param <T> the produced type representation
 * @param <I> the integer-parameter representation
 */
public interface ExtendedTypeCreator<T, I> {
  /**
   * Creates a fixed-length character type with a parameterized length.
   *
   * @param len the length parameter
   * @return the fixed-char type
   */
  T fixedCharE(I len);

  /**
   * Creates a variable-length character type with a parameterized length.
   *
   * @param len the length parameter
   * @return the varchar type
   */
  T varCharE(I len);

  /**
   * Creates a fixed-length binary type with a parameterized length.
   *
   * @param len the length parameter
   * @return the fixed-binary type
   */
  T fixedBinaryE(I len);

  /**
   * Creates a decimal type with parameterized precision and scale.
   *
   * @param precision the precision parameter
   * @param scale the scale parameter
   * @return the decimal type
   */
  T decimalE(I precision, I scale);

  /**
   * Creates a struct type from the given field types.
   *
   * @param types the field types
   * @return the struct type
   */
  T structE(T... types);

  /**
   * Creates a struct type from the given field types.
   *
   * @param types the field types
   * @return the struct type
   */
  T structE(Iterable<? extends T> types);

  /**
   * Creates a list type with the given element type.
   *
   * @param type the element type
   * @return the list type
   */
  T listE(T type);

  /**
   * Creates a map type with the given key and value types.
   *
   * @param key the key type
   * @param value the value type
   * @return the map type
   */
  T mapE(T key, T value);

  /**
   * Creates a function type with the given parameter types and return type.
   *
   * @param parameterTypes the parameter types
   * @param returnType the return type
   * @return the function type
   */
  T funcE(Iterable<? extends T> parameterTypes, T returnType);
}
