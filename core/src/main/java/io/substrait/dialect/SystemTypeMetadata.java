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
  public abstract Optional<String> name();

  @JsonProperty("supported_as_column")
  public abstract Optional<Boolean> supportedAsColumn();

  public static ImmutableSystemTypeMetadata.Builder builder() {
    return ImmutableSystemTypeMetadata.builder();
  }
}
