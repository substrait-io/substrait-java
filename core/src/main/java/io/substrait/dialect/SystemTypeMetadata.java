package io.substrait.dialect;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Optional;
import org.immutables.value.Value;

/** System-specific metadata for a dialect type. */
@JsonDeserialize(as = ImmutableSystemTypeMetadata.class)
@JsonSerialize(as = ImmutableSystemTypeMetadata.class)
@Value.Immutable
public abstract class SystemTypeMetadata {
  /**
   * The name of the type in the system's own syntax, if any.
   *
   * @return the optional system name
   */
  public abstract Optional<String> name();

  /**
   * Whether the type is supported as a column type, if specified.
   *
   * @return the optional column-support flag
   */
  @JsonProperty("supported_as_column")
  public abstract Optional<Boolean> supportedAsColumn();

  /**
   * Creates a builder for {@link SystemTypeMetadata}.
   *
   * @return a new builder
   */
  public static ImmutableSystemTypeMetadata.Builder builder() {
    return ImmutableSystemTypeMetadata.builder();
  }
}
