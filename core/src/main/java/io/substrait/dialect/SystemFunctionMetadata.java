package io.substrait.dialect;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Optional;
import org.immutables.value.Value;

/** System-specific metadata for a dialect function. */
@JsonDeserialize(as = ImmutableSystemFunctionMetadata.class)
@JsonSerialize(as = ImmutableSystemFunctionMetadata.class)
@Value.Immutable
public abstract class SystemFunctionMetadata {
  /**
   * The name of the function in the system's own syntax, if any.
   *
   * @return the optional system name
   */
  public abstract Optional<String> name();

  /**
   * The notation in which the function is written in the system's own syntax.
   *
   * @return the notation
   */
  @Value.Default
  public Notation notation() {
    return Notation.FUNCTION;
  }

  /**
   * Creates a builder for {@link SystemFunctionMetadata}.
   *
   * @return a new builder
   */
  public static ImmutableSystemFunctionMetadata.Builder builder() {
    return ImmutableSystemFunctionMetadata.builder();
  }
}
