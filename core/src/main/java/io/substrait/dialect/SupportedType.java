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
  /**
   * The kind of type this entry describes.
   *
   * @return the type kind
   */
  public abstract TypeKind type();

  /**
   * Free-form metadata associated with the type, if any.
   *
   * @return the optional metadata
   */
  public abstract Optional<Map<String, Object>> metadata();

  /**
   * System-specific metadata for the type, if any.
   *
   * @return the optional system metadata
   */
  public abstract Optional<SystemTypeMetadata> systemMetadata();

  /**
   * The maximum precision supported for the type, if constrained. Applies to the
   * subsecond-precision temporal types ({@code PRECISION_TIME}, {@code PRECISION_TIMESTAMP}, {@code
   * PRECISION_TIMESTAMP_TZ}, {@code INTERVAL_COMPOUND}, {@code INTERVAL_DAY}) and, together with
   * {@link #maxScale()}, to {@code DECIMAL}.
   *
   * @return the optional maximum precision
   */
  public abstract Optional<Integer> maxPrecision();

  /**
   * The maximum scale supported for a {@code DECIMAL} type, if constrained.
   *
   * @return the optional maximum scale
   */
  public abstract Optional<Integer> maxScale();

  /**
   * The maximum length supported for a variable- or fixed-length type ({@code FIXED_BINARY}, {@code
   * VARCHAR}, {@code FIXED_CHAR}), if constrained.
   *
   * @return the optional maximum length
   */
  public abstract Optional<Integer> maxLength();

  /**
   * Dependency (alias) where a {@code USER_DEFINED} type is declared.
   *
   * @return the optional dependency alias
   */
  public abstract Optional<String> source();

  /**
   * The name of a {@code USER_DEFINED} type as declared in the extension it is defined in.
   *
   * @return the optional type name
   */
  public abstract Optional<String> name();

  /**
   * Whether this entry can be written as a bare enum string (no extra configuration).
   *
   * @return {@code true} if the entry carries no configuration
   */
  public boolean isBare() {
    return type() != TypeKind.USER_DEFINED
        && !metadata().isPresent()
        && !systemMetadata().isPresent()
        && !maxPrecision().isPresent()
        && !maxScale().isPresent()
        && !maxLength().isPresent()
        && !source().isPresent()
        && !name().isPresent();
  }

  /**
   * Creates a configuration-free entry for the given kind.
   *
   * @param type the type kind
   * @return a new {@link SupportedType}
   */
  public static SupportedType of(TypeKind type) {
    return builder().type(type).build();
  }

  /**
   * Creates a builder for {@link SupportedType}.
   *
   * @return a new builder
   */
  public static ImmutableSupportedType.Builder builder() {
    return ImmutableSupportedType.builder();
  }
}
