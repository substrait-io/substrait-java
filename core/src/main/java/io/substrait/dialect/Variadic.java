package io.substrait.dialect;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.OptionalInt;
import org.immutables.value.Value;

/**
 * Variadic argument bounds for a dialect function. The dialect schema counts every argument of a
 * call ("Minimum number of arguments required"), where an extension's variadic behavior counts
 * repetitions of the last argument only, so the two differ for a function with arguments before its
 * repeated one.
 */
@JsonDeserialize(as = ImmutableVariadic.class)
@JsonSerialize(as = ImmutableVariadic.class)
@Value.Immutable
public abstract class Variadic {
  /**
   * The minimum number of arguments, if bounded.
   *
   * @return the optional minimum
   */
  public abstract OptionalInt min();

  /**
   * The maximum number of arguments, if bounded.
   *
   * @return the optional maximum
   */
  public abstract OptionalInt max();

  /**
   * Creates a builder for {@link Variadic}.
   *
   * @return a new builder
   */
  public static ImmutableVariadic.Builder builder() {
    return ImmutableVariadic.builder();
  }
}
