package io.substrait.dialect;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.OptionalInt;
import org.immutables.value.Value;

/** Variadic argument bounds for a dialect function. */
@JsonDeserialize(as = ImmutableVariadic.class)
@JsonSerialize(as = ImmutableVariadic.class)
@Value.Immutable
public abstract class Variadic {
  public abstract OptionalInt min();

  public abstract OptionalInt max();

  public static ImmutableVariadic.Builder builder() {
    return ImmutableVariadic.builder();
  }
}
