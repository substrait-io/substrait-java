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

  /**
   * The name of the dialect, if any.
   *
   * @return the optional dialect name
   */
  public abstract Optional<String> name();

  /**
   * Free-form metadata associated with the dialect, if any.
   *
   * @return the optional metadata
   */
  public abstract Optional<Map<String, Object>> metadata();

  /**
   * The dependencies referenced by the dialect, keyed by alias.
   *
   * @return the dependency aliases mapped to their URIs
   */
  public abstract Map<String, String> dependencies();

  /**
   * The types supported by the dialect.
   *
   * @return the supported types
   */
  @JsonProperty("supported_types")
  public abstract List<SupportedType> supportedTypes();

  /**
   * The relations supported by the dialect.
   *
   * @return the supported relations
   */
  @JsonProperty("supported_relations")
  public abstract List<SupportedRelation> supportedRelations();

  /**
   * The expressions supported by the dialect.
   *
   * @return the supported expressions
   */
  @JsonProperty("supported_expressions")
  public abstract List<SupportedExpression> supportedExpressions();

  /**
   * The scalar functions supported by the dialect.
   *
   * @return the supported scalar functions
   */
  @JsonProperty("supported_scalar_functions")
  public abstract List<DialectFunction> supportedScalarFunctions();

  /**
   * The aggregate functions supported by the dialect.
   *
   * @return the supported aggregate functions
   */
  @JsonProperty("supported_aggregate_functions")
  public abstract List<DialectFunction> supportedAggregateFunctions();

  /**
   * The window functions supported by the dialect.
   *
   * @return the supported window functions
   */
  @JsonProperty("supported_window_functions")
  public abstract List<DialectFunction> supportedWindowFunctions();

  /**
   * The execution-behavior configuration of the dialect, if any.
   *
   * @return the optional execution behavior
   */
  @JsonProperty("supported_execution_behavior")
  public abstract Optional<ExecutionBehavior> supportedExecutionBehavior();

  /**
   * Creates a builder for {@link Dialect}.
   *
   * @return a new builder
   */
  public static ImmutableDialect.Builder builder() {
    return ImmutableDialect.builder();
  }

  /**
   * Parse a dialect from YAML content.
   *
   * @param content the YAML content
   * @return the parsed dialect
   */
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
   *
   * @param stream the YAML input stream
   * @return the parsed dialect
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

  /**
   * Parse a dialect from a YAML file on disk.
   *
   * @param path the path to the YAML file
   * @return the parsed dialect
   */
  public static Dialect loadFromFile(Path path) {
    try {
      return DialectJsonSupport.MAPPER.readValue(path.toFile(), Dialect.class);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Parse a dialect from a classpath resource.
   *
   * @param resourcePath the classpath resource path
   * @return the parsed dialect
   */
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

  /**
   * Serialize a dialect to YAML.
   *
   * @param dialect the dialect to serialize
   * @return the YAML representation
   */
  public static String toYaml(Dialect dialect) {
    try {
      return DialectJsonSupport.MAPPER.writeValueAsString(dialect);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
