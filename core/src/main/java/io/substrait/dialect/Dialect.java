package io.substrait.dialect;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * A Substrait dialect: a description of what a target execution system supports — which types,
 * relations, expressions and functions, along with per-feature configuration (join types, set
 * operations, cast failure behavior, ...). The model maps the schema published at {@code
 * substrait/text/dialect_schema.yaml}.
 *
 * <p>Build a dialect with {@link #builder()} and the sibling entry types and serialize it with
 * {@link #toYaml(Dialect)}; parse one with {@link #load(String)} and friends.
 */
@JsonDeserialize(as = ImmutableDialect.class)
@JsonSerialize(as = ImmutableDialect.class)
@Value.Immutable
public abstract class Dialect {

  public abstract Optional<String> name();

  public abstract Optional<Map<String, Object>> metadata();

  public abstract Map<String, String> dependencies();

  @JsonProperty("supported_types")
  public abstract List<SupportedType> supportedTypes();

  @JsonProperty("supported_relations")
  public abstract List<SupportedRelation> supportedRelations();

  @JsonProperty("supported_expressions")
  public abstract List<SupportedExpression> supportedExpressions();

  @JsonProperty("supported_scalar_functions")
  public abstract List<DialectFunction> supportedScalarFunctions();

  @JsonProperty("supported_aggregate_functions")
  public abstract List<DialectFunction> supportedAggregateFunctions();

  @JsonProperty("supported_window_functions")
  public abstract List<DialectFunction> supportedWindowFunctions();

  @JsonProperty("supported_execution_behavior")
  public abstract Optional<ExecutionBehavior> supportedExecutionBehavior();

  public static ImmutableDialect.Builder builder() {
    return ImmutableDialect.builder();
  }

  /** Parse a dialect from YAML content. */
  public static Dialect load(String content) {
    try {
      return DialectJsonSupport.MAPPER.readValue(content, Dialect.class);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Parse a dialect from a YAML stream. The caller retains ownership of the stream; it is not
   * closed by this method.
   */
  public static Dialect load(InputStream stream) {
    try {
      return DialectJsonSupport.MAPPER
          .readerFor(Dialect.class)
          .without(JsonParser.Feature.AUTO_CLOSE_SOURCE)
          .readValue(stream);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Parse a dialect from a YAML file on disk. */
  public static Dialect loadFromFile(Path path) {
    try {
      return DialectJsonSupport.MAPPER.readValue(path.toFile(), Dialect.class);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Parse a dialect from a classpath resource. */
  public static Dialect loadResource(String resourcePath) {
    try (InputStream stream = Dialect.class.getResourceAsStream(resourcePath)) {
      if (stream == null) {
        throw new IllegalArgumentException("Dialect resource not found: " + resourcePath);
      }
      return load(stream);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Serialize a dialect to YAML. */
  public static String toYaml(Dialect dialect) {
    try {
      return DialectJsonSupport.MAPPER.writeValueAsString(dialect);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
