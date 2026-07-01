package io.substrait.dialect;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

/** A function supported by a dialect, referencing a dependency alias. */
@JsonDeserialize(as = ImmutableDialectFunction.class)
@JsonSerialize(as = ImmutableDialectFunction.class)
@Value.Immutable
public abstract class DialectFunction {
  /**
   * Dependency (alias) in which the function is declared.
   *
   * @return the dependency alias
   */
  public abstract String source();

  /**
   * The name of the function as declared in the extension it is defined in.
   *
   * @return the function name
   */
  public abstract String name();

  /**
   * Free-form metadata associated with the function, if any.
   *
   * @return the optional metadata
   */
  public abstract Optional<Map<String, Object>> metadata();

  /**
   * System-specific metadata for the function, if any.
   *
   * @return the optional system metadata
   */
  @JsonProperty("system_metadata")
  public abstract Optional<SystemFunctionMetadata> systemMetadata();

  /**
   * The options required when invoking the function, if any.
   *
   * @return the optional required options
   */
  @JsonProperty("required_options")
  public abstract Optional<Map<String, Object>> requiredOptions();

  /**
   * One or more implementations supported by this function, identified by argument signatures.
   *
   * @return the supported implementation signatures
   */
  @JsonProperty("supported_impls")
  public abstract List<String> supportedImpls();

  /**
   * The variadic argument bounds of the function, if any.
   *
   * @return the optional variadic bounds
   */
  public abstract Optional<Variadic> variadic();

  /**
   * Creates a builder for {@link DialectFunction}.
   *
   * @return a new builder
   */
  public static ImmutableDialectFunction.Builder builder() {
    return ImmutableDialectFunction.builder();
  }
}
