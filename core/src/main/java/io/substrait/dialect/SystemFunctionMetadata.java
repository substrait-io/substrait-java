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
  public abstract Optional<String> name();

  @Value.Default
  public Notation notation() {
    return Notation.FUNCTION;
  }

  public static ImmutableSystemFunctionMetadata.Builder builder() {
    return ImmutableSystemFunctionMetadata.builder();
  }
}
