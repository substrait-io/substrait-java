package io.substrait.dialect;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * A {@code supported_types} entry. Serializes as a bare enum string when it carries no
 * configuration, or as a configuration object otherwise.
 */
@JsonDeserialize(using = SupportedTypeDeserializer.class)
@JsonSerialize(using = SupportedTypeSerializer.class)
@Value.Immutable
public abstract class SupportedType {
  public abstract TypeKind type();

  public abstract Optional<Map<String, Object>> metadata();

  public abstract Optional<SystemTypeMetadata> systemMetadata();

  public abstract Optional<Integer> maxPrecision();

  /** Dependency (alias) where a {@code USER_DEFINED} type is declared. */
  public abstract Optional<String> source();

  /** The name of a {@code USER_DEFINED} type as declared in the extension it is defined in. */
  public abstract Optional<String> name();

  /** Whether this entry can be written as a bare enum string (no extra configuration). */
  public boolean isBare() {
    return type() != TypeKind.USER_DEFINED
        && !metadata().isPresent()
        && !systemMetadata().isPresent()
        && !maxPrecision().isPresent()
        && !source().isPresent()
        && !name().isPresent();
  }

  public static SupportedType of(TypeKind type) {
    return builder().type(type).build();
  }

  public static ImmutableSupportedType.Builder builder() {
    return ImmutableSupportedType.builder();
  }
}
