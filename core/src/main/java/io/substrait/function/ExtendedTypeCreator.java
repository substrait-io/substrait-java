package io.substrait.function;

public interface ExtendedTypeCreator<T, I> {
  T fixedCharE(I len);

  T varCharE(I len);

  T fixedBinaryE(I len);

  T decimalE(I precision, I scale);

  T structE(T... types);

  T structE(Iterable<? extends T> types);

  T listE(T type);

  T mapE(T key, T value);
}
