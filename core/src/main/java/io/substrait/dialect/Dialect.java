package io.substrait.dialect;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Scanner;
import java.util.regex.Pattern;
import org.immutables.value.Value;

/**
 * Classes used to create and consume Substrait dialect YAML files.
 *
 * <p>A dialect describes what a target execution system supports: which types, relations,
 * expressions and functions, along with per-feature configuration (join types, set operations, cast
 * failure behavior, ...). The model maps the schema published at {@code
 * substrait/text/dialect_schema.yaml}.
 *
 * <p>Build a dialect with the nested builders and serialize it with {@link
 * #toYaml(DialectDocument)}; parse one with {@link #load(String)} and friends.
 */
@Value.Enclosing
public class Dialect {

  // `\A` means beginning of input. Using it as a delimiter in a scanner reads in the whole file.
  private static final Pattern READ_WHOLE_FILE = Pattern.compile("\\A");

  private Dialect() {}

  private static ObjectMapper objectMapper() {
    return new ObjectMapper(new YAMLFactory())
        .registerModule(new Jdk8Module())
        .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
        // Omit absent Optionals and empty collections so that unset sections are not emitted.
        // The custom (de)serializers for the polymorphic unions write their fields explicitly and
        // are unaffected by this inclusion setting.
        .setDefaultPropertyInclusion(JsonInclude.Include.NON_EMPTY);
  }

  /** Parse a dialect from YAML content. */
  public static DialectDocument load(String content) {
    try {
      return objectMapper().readValue(content, DialectDocument.class);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Parse a dialect from a YAML stream. */
  public static DialectDocument load(InputStream stream) {
    try (Scanner scanner = new Scanner(stream, StandardCharsets.UTF_8.name())) {
      scanner.useDelimiter(READ_WHOLE_FILE);
      String content = scanner.hasNext() ? scanner.next() : "";
      return load(content);
    }
  }

  /** Parse a dialect from a YAML file on disk. */
  public static DialectDocument loadFromFile(Path path) {
    try {
      return load(new String(Files.readAllBytes(path), StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Parse a dialect from a classpath resource. */
  public static DialectDocument loadResource(String resourcePath) {
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
  public static String toYaml(DialectDocument dialect) {
    try {
      return objectMapper().writeValueAsString(dialect);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  // ---------------------------------------------------------------------------
  // Root document
  // ---------------------------------------------------------------------------

  @JsonDeserialize(as = ImmutableDialect.DialectDocument.class)
  @JsonSerialize(as = ImmutableDialect.DialectDocument.class)
  @Value.Immutable
  public abstract static class DialectDocument {
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

    public static ImmutableDialect.DialectDocument.Builder builder() {
      return ImmutableDialect.DialectDocument.builder();
    }
  }

  // ---------------------------------------------------------------------------
  // Functions
  // ---------------------------------------------------------------------------

  @JsonDeserialize(as = ImmutableDialect.DialectFunction.class)
  @JsonSerialize(as = ImmutableDialect.DialectFunction.class)
  @Value.Immutable
  public abstract static class DialectFunction {
    /** Dependency (alias) in which the function is declared. */
    public abstract String source();

    /** The name of the function as declared in the extension it is defined in. */
    public abstract String name();

    public abstract Optional<Map<String, Object>> metadata();

    @JsonProperty("system_metadata")
    public abstract Optional<SystemFunctionMetadata> systemMetadata();

    @JsonProperty("required_options")
    public abstract Optional<Map<String, Object>> requiredOptions();

    /**
     * One or more implementations supported by this function, identified by argument signatures.
     */
    @JsonProperty("supported_impls")
    public abstract List<String> supportedImpls();

    public abstract Optional<Variadic> variadic();

    public static ImmutableDialect.DialectFunction.Builder builder() {
      return ImmutableDialect.DialectFunction.builder();
    }
  }

  @JsonDeserialize(as = ImmutableDialect.SystemFunctionMetadata.class)
  @JsonSerialize(as = ImmutableDialect.SystemFunctionMetadata.class)
  @Value.Immutable
  public abstract static class SystemFunctionMetadata {
    public abstract Optional<String> name();

    @Value.Default
    public Notation notation() {
      return Notation.FUNCTION;
    }

    public static ImmutableDialect.SystemFunctionMetadata.Builder builder() {
      return ImmutableDialect.SystemFunctionMetadata.builder();
    }
  }

  @JsonDeserialize(as = ImmutableDialect.Variadic.class)
  @JsonSerialize(as = ImmutableDialect.Variadic.class)
  @Value.Immutable
  public abstract static class Variadic {
    public abstract OptionalInt min();

    public abstract OptionalInt max();

    public static ImmutableDialect.Variadic.Builder builder() {
      return ImmutableDialect.Variadic.builder();
    }
  }

  public enum Notation {
    INFIX,
    POSTFIX,
    PREFIX,
    FUNCTION
  }

  // ---------------------------------------------------------------------------
  // Types
  // ---------------------------------------------------------------------------

  @JsonDeserialize(using = SupportedTypeDeserializer.class)
  @JsonSerialize(using = SupportedTypeSerializer.class)
  @Value.Immutable
  public abstract static class SupportedType {
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

    public static ImmutableDialect.SupportedType.Builder builder() {
      return ImmutableDialect.SupportedType.builder();
    }
  }

  @JsonDeserialize(as = ImmutableDialect.SystemTypeMetadata.class)
  @JsonSerialize(as = ImmutableDialect.SystemTypeMetadata.class)
  @Value.Immutable
  public abstract static class SystemTypeMetadata {
    public abstract Optional<String> name();

    @JsonProperty("supported_as_column")
    public abstract Optional<Boolean> supportedAsColumn();

    public static ImmutableDialect.SystemTypeMetadata.Builder builder() {
      return ImmutableDialect.SystemTypeMetadata.builder();
    }
  }

  public enum TypeKind {
    BOOL,
    I8,
    I16,
    I32,
    I64,
    FP32,
    FP64,
    BINARY,
    FIXED_BINARY,
    STRING,
    VARCHAR,
    FIXED_CHAR,
    PRECISION_TIME,
    PRECISION_TIMESTAMP,
    PRECISION_TIMESTAMP_TZ,
    DATE,
    TIME,
    INTERVAL_COMPOUND,
    INTERVAL_DAY,
    INTERVAL_YEAR,
    UUID,
    DECIMAL,
    STRUCT,
    LIST,
    MAP,
    USER_DEFINED
  }

  // ---------------------------------------------------------------------------
  // Relations
  // ---------------------------------------------------------------------------

  @JsonDeserialize(using = SupportedRelationDeserializer.class)
  @JsonSerialize(using = SupportedRelationSerializer.class)
  @Value.Immutable
  public abstract static class SupportedRelation {
    public abstract RelationKind relation();

    public abstract Optional<Map<String, Object>> metadata();

    /**
     * Join types for {@code JOIN}, {@code HASH_JOIN}, {@code MERGE_JOIN}, {@code NESTED_LOOP_JOIN}.
     */
    public abstract List<JoinType> joinTypes();

    /** Read types for {@code READ}. */
    public abstract List<ReadType> readTypes();

    /** Set operations for {@code SET}. */
    public abstract List<SetOperation> operations();

    /** Write types for {@code WRITE} (serialized as {@code write_types}). */
    public abstract List<WriteType> writeTypes();

    /** Operable object types for {@code DDL} (also serialized as {@code write_types}). */
    public abstract List<DdlWriteType> ddlWriteTypes();

    /** Exchange kinds for {@code EXCHANGE}. */
    public abstract List<ExchangeKind> kinds();

    /** Field types for {@code EXPAND}. */
    public abstract List<ExpandFieldType> fieldTypes();

    /** Supported message type URIs for {@code EXTENSION_SINGLE}/{@code MULTI}/{@code LEAF}. */
    public abstract List<String> messageTypes();

    /**
     * Whether this entry can be written as a bare enum string. Extension relations are never bare:
     * they are absent from the schema's bare-enum list.
     */
    public boolean isBare() {
      switch (relation()) {
        case EXTENSION_SINGLE:
        case EXTENSION_MULTI:
        case EXTENSION_LEAF:
          return false;
        default:
          break;
      }
      return !metadata().isPresent()
          && joinTypes().isEmpty()
          && readTypes().isEmpty()
          && operations().isEmpty()
          && writeTypes().isEmpty()
          && ddlWriteTypes().isEmpty()
          && kinds().isEmpty()
          && fieldTypes().isEmpty()
          && messageTypes().isEmpty();
    }

    public static SupportedRelation of(RelationKind relation) {
      return builder().relation(relation).build();
    }

    public static ImmutableDialect.SupportedRelation.Builder builder() {
      return ImmutableDialect.SupportedRelation.builder();
    }
  }

  public enum RelationKind {
    READ,
    FILTER,
    FETCH,
    AGGREGATE,
    SORT,
    JOIN,
    PROJECT,
    SET,
    CROSS,
    REFERENCE,
    WRITE,
    DDL,
    UPDATE,
    HASH_JOIN,
    MERGE_JOIN,
    NESTED_LOOP_JOIN,
    CONSISTENT_PARTITION_WINDOW,
    EXCHANGE,
    EXPAND,
    EXTENSION_SINGLE,
    EXTENSION_MULTI,
    EXTENSION_LEAF
  }

  public enum JoinType {
    INNER,
    OUTER,
    LEFT,
    RIGHT,
    LEFT_SEMI,
    RIGHT_SEMI,
    LEFT_ANTI,
    RIGHT_ANTI,
    LEFT_SINGLE,
    RIGHT_SINGLE,
    LEFT_MARK,
    RIGHT_MARK
  }

  public enum ReadType {
    VIRTUAL_TABLE,
    LOCAL_FILES,
    NAMED_TABLE,
    EXTENSION_TABLE,
    ICEBERG_TABLE
  }

  public enum SetOperation {
    MINUS_PRIMARY,
    MINUS_PRIMARY_ALL,
    MINUS_MULTISET,
    INTERSECTION_PRIMARY,
    INTERSECTION_MULTISET,
    INTERSECTION_MULTISET_ALL,
    UNION_DISTINCT,
    UNION_ALL
  }

  public enum WriteType {
    NAMED_TABLE,
    EXTENSION_TABLE
  }

  public enum DdlWriteType {
    NAMED_OBJECT,
    EXTENSION_OBJECT
  }

  public enum ExchangeKind {
    SCATTER_BY_FIELDS,
    SINGLE_TARGET,
    MULTI_TARGET,
    ROUND_ROBIN,
    BROADCAST
  }

  public enum ExpandFieldType {
    SWITCHING_FIELD,
    CONSTANT_FIELD
  }

  // ---------------------------------------------------------------------------
  // Expressions
  // ---------------------------------------------------------------------------

  @JsonDeserialize(using = SupportedExpressionDeserializer.class)
  @JsonSerialize(using = SupportedExpressionSerializer.class)
  @Value.Immutable
  public abstract static class SupportedExpression {
    public abstract ExpressionKind expression();

    public abstract Optional<Map<String, Object>> metadata();

    /** Permissible failure options for {@code CAST}. */
    public abstract List<CastFailureOption> failureOptions();

    /** Subquery types for {@code SUBQUERY}. */
    public abstract List<SubqueryType> subqueryTypes();

    /** Nested types for {@code NESTED}. */
    public abstract List<NestedType> nestedTypes();

    /** Variable types for {@code EXECUTION_CONTEXT_VARIABLE}. */
    public abstract List<VariableType> variableTypes();

    /** Whether this entry can be written as a bare enum string (no extra configuration). */
    public boolean isBare() {
      return !metadata().isPresent()
          && failureOptions().isEmpty()
          && subqueryTypes().isEmpty()
          && nestedTypes().isEmpty()
          && variableTypes().isEmpty();
    }

    public static SupportedExpression of(ExpressionKind expression) {
      return builder().expression(expression).build();
    }

    public static ImmutableDialect.SupportedExpression.Builder builder() {
      return ImmutableDialect.SupportedExpression.builder();
    }
  }

  public enum ExpressionKind {
    LITERAL,
    SELECTION,
    SCALAR_FUNCTION,
    WINDOW_FUNCTION,
    IF_THEN,
    SWITCH,
    SINGULAR_OR_LIST,
    MULTI_OR_LIST,
    CAST,
    SUBQUERY,
    NESTED,
    DYNAMIC_PARAMETER,
    EXECUTION_CONTEXT_VARIABLE
  }

  public enum CastFailureOption {
    RETURN_NULL,
    THROW_EXCEPTION
  }

  public enum SubqueryType {
    SCALAR,
    IN_PREDICATE,
    SET_PREDICATE,
    SET_COMPARISON
  }

  public enum NestedType {
    STRUCT,
    LIST,
    MAP
  }

  public enum VariableType {
    CURRENT_TIMESTAMP,
    CURRENT_TIMEZONE,
    CURRENT_DATE
  }

  // ---------------------------------------------------------------------------
  // Execution behavior
  // ---------------------------------------------------------------------------

  @JsonDeserialize(as = ImmutableDialect.ExecutionBehavior.class)
  @JsonSerialize(as = ImmutableDialect.ExecutionBehavior.class)
  @Value.Immutable
  public abstract static class ExecutionBehavior {
    @JsonProperty("supported_variable_evaluation_mode")
    public abstract List<VariableEvaluationMode> supportedVariableEvaluationMode();

    public static ImmutableDialect.ExecutionBehavior.Builder builder() {
      return ImmutableDialect.ExecutionBehavior.builder();
    }
  }

  public enum VariableEvaluationMode {
    PER_PLAN,
    PER_RECORD
  }
}
