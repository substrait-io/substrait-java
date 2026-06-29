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
  /** Dependency (alias) in which the function is declared. */
  public abstract String source();

  /** The name of the function as declared in the extension it is defined in. */
  public abstract String name();

  public abstract Optional<Map<String, Object>> metadata();

  @JsonProperty("system_metadata")
  public abstract Optional<SystemFunctionMetadata> systemMetadata();

  @JsonProperty("required_options")
  public abstract Optional<Map<String, Object>> requiredOptions();

  /** One or more implementations supported by this function, identified by argument signatures. */
  @JsonProperty("supported_impls")
  public abstract List<String> supportedImpls();

  public abstract Optional<Variadic> variadic();

  public static ImmutableDialectFunction.Builder builder() {
    return ImmutableDialectFunction.builder();
  }
}
