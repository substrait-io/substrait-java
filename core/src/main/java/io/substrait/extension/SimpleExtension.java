package io.substrait.extension;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.substrait.expression.Expression;
import io.substrait.function.ParameterizedType;
import io.substrait.function.ToTypeString;
import io.substrait.function.TypeExpression;
import io.substrait.type.Deserializers;
import io.substrait.type.TypeExpressionEvaluator;
import io.substrait.util.Util;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Scanner;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.immutables.value.Value;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Classes used to deserialize YAML extension files. Handles functions and types. */
@Value.Enclosing
public class SimpleExtension {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleExtension.class);

  // Key for looking up URN in InjectableValues
  /** Metadata key identifying the extension URN. */
  public static final String URN_LOCATOR_KEY = "urn";

  private static final Predicate<String> URN_CHECKER =
      Pattern.compile("^extension:[^:]+:[^:]+$").asPredicate();

  // `\A` means beginning of input. Using it as a delimiter in a scanner reads in the whole file.
  private static Pattern READ_WHOLE_FILE = Pattern.compile("\\A");

  private static void validateUrn(String urn) {
    if (urn == null || urn.trim().isEmpty()) {
      throw new IllegalArgumentException("URN cannot be null or empty");
    }
    if (!URN_CHECKER.test(urn)) {
      throw new IllegalArgumentException(
          "URN must follow format 'extension:<namespace>:<name>', got: " + urn);
    }
  }

  private static ObjectMapper objectMapper(String urn) {
    InjectableValues.Std iv = new InjectableValues.Std();
    iv.addValue(URN_LOCATOR_KEY, urn);

    return new ObjectMapper(new YAMLFactory())
        .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
        .registerModule(new Jdk8Module())
        .registerModule(Deserializers.MODULE)
        .setInjectableValues(iv);
  }

  /** Enumerates the supported nullability values. */
  public enum Nullability {
    /** The MIRROR value. */
    MIRROR,
    /** The DECLARED_OUTPUT value. */
    DECLARED_OUTPUT,
    /** The DISCRETE value. */
    DISCRETE
  }

  /** Enumerates the supported decomposability values. */
  public enum Decomposability {
    /** The NONE value. */
    NONE,
    /** The ONE value. */
    ONE,
    /** The MANY value. */
    MANY
  }

  /** Enumerates the supported window type values. */
  public enum WindowType {
    /** The PARTITION value. */
    PARTITION,
    /** The STREAMING value. */
    STREAMING
  }

  private SimpleExtension() {}

  /** Describes an argument provided by a simple extension. */
  @JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION)
  @JsonSubTypes({
    @JsonSubTypes.Type(ValueArgument.class),
    @JsonSubTypes.Type(TypeArgument.class),
    @JsonSubTypes.Type(EnumArgument.class)
  })
  public interface Argument {
    /**
     * Returns the to Type String.
     *
     * @return the to Type String
     */
    String toTypeString();

    /**
     * Returns the name.
     *
     * @return the name
     */
    @JsonProperty()
    @Nullable String name();

    /**
     * Returns the description.
     *
     * @return the description
     */
    @JsonProperty()
    @Nullable String description();

    /**
     * Returns the required.
     *
     * @return the required
     */
    boolean required();
  }

  /** Describes an option provided by a simple extension. */
  @JsonDeserialize(as = ImmutableSimpleExtension.Option.class)
  @JsonSerialize(as = ImmutableSimpleExtension.Option.class)
  @Value.Immutable
  public interface Option {
    /**
     * Returns the description.
     *
     * @return the description
     */
    Optional<String> getDescription();

    /**
     * Returns the values.
     *
     * @return the values
     */
    List<String> getValues();
  }

  /**
   * Deprecation information for an extension entry (type, function, or function implementation).
   *
   * <p>Consumers of extension files are not required to understand or validate deprecation fields;
   * the information is provided so tooling can surface deprecation warnings.
   */
  @JsonDeserialize(as = ImmutableSimpleExtension.DeprecationStatus.class)
  @JsonSerialize(as = ImmutableSimpleExtension.DeprecationStatus.class)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @Value.Immutable
  public interface DeprecationStatus {
    /**
     * The version at which the entry was deprecated, as a core semantic version string (e.g. {@code
     * "1.2.0"}).
     */
    /**
     * Returns the since.
     *
     * @return the since
     */
    @JsonProperty(required = true)
    String since();

    /** Optional human-readable description of why the entry was deprecated. */
    /**
     * Returns the reason.
     *
     * @return the reason
     */
    Optional<String> reason();

    /** Optional arbitrary data provided by the extension author. */
    /**
     * Returns the metadata.
     *
     * @return the metadata
     */
    Optional<Map<String, Object>> metadata();
  }

  /** Describes a value argument provided by a simple extension. */
  @JsonSerialize(as = ImmutableSimpleExtension.ValueArgument.class)
  @JsonDeserialize(as = ImmutableSimpleExtension.ValueArgument.class)
  @Value.Immutable
  public abstract static class ValueArgument implements Argument {

    /**
     * Returns the value.
     *
     * @return the value
     */
    @JsonProperty(required = true)
    public abstract ParameterizedType value();

    /**
     * Returns the constant.
     *
     * @return the constant
     */
    @JsonProperty()
    @Nullable
    public abstract Boolean constant();

    @Override
    public String toTypeString() {
      return value().accept(ToTypeString.INSTANCE);
    }

    @Override
    public boolean required() {
      return true;
    }

    /**
     * Creates a new builder.
     *
     * @return a new builder
     */
    public static ImmutableSimpleExtension.ValueArgument.Builder builder() {
      return ImmutableSimpleExtension.ValueArgument.builder();
    }
  }

  /** Describes a type argument provided by a simple extension. */
  @JsonSerialize(as = ImmutableSimpleExtension.TypeArgument.class)
  @JsonDeserialize(as = ImmutableSimpleExtension.TypeArgument.class)
  @Value.Immutable
  public abstract static class TypeArgument implements Argument {

    /**
     * Returns the type.
     *
     * @return the type
     */
    @JsonProperty(required = true)
    public abstract ParameterizedType type();

    @Override
    public String toTypeString() {
      return "type";
    }

    @Override
    public boolean required() {
      return true;
    }

    /**
     * Creates a new builder.
     *
     * @return a new builder
     */
    public static ImmutableSimpleExtension.TypeArgument.Builder builder() {
      return ImmutableSimpleExtension.TypeArgument.builder();
    }
  }

  /**
   * An enum argument is required to be known within the plan (no field references can be used).
   * These are distinct from <i>Enum data types</i>; which are just like any other type and could be
   * reasonably expressed by referencing a preexisting column that contains those values.
   *
   * <p>For more details see <a
   * href="https://github.com/substrait-io/substrait-java/pull/55#issuecomment-1154484254">comments
   * in this issue</a>
   */
  @JsonSerialize(as = ImmutableSimpleExtension.EnumArgument.class)
  @JsonDeserialize(as = ImmutableSimpleExtension.EnumArgument.class)
  @Value.Immutable
  public abstract static class EnumArgument implements Argument {

    /**
     * Returns the options.
     *
     * @return the options
     */
    @JsonProperty(required = true)
    public abstract List<String> options();

    @Override
    public boolean required() {
      return true;
    }

    @Override
    public String toTypeString() {
      return "req";
    }

    /**
     * Creates a new builder.
     *
     * @return a new builder
     */
    public static ImmutableSimpleExtension.EnumArgument.Builder builder() {
      return ImmutableSimpleExtension.EnumArgument.builder();
    }
  }

  /** Describes an anchor provided by a simple extension. */
  public interface Anchor {
    /**
     * Returns the urn.
     *
     * @return the urn
     */
    String urn();

    /**
     * Returns the key.
     *
     * @return the key
     */
    String key();
  }

  /** Describes a function anchor provided by a simple extension. */
  @Value.Immutable
  public interface FunctionAnchor extends Anchor {
    /**
     * Creates the corresponding of instance.
     *
     * @param urn the urn
     * @param key the key
     * @return the of
     */
    static FunctionAnchor of(String urn, String key) {
      return ImmutableSimpleExtension.FunctionAnchor.builder().urn(urn).key(key).build();
    }
  }

  /** Describes a type anchor provided by a simple extension. */
  @Value.Immutable
  public interface TypeAnchor extends Anchor {
    /**
     * Creates the corresponding of instance.
     *
     * @param urn the urn
     * @param name the name
     * @return the of
     */
    static TypeAnchor of(String urn, String name) {
      return ImmutableSimpleExtension.TypeAnchor.builder().urn(urn).key(name).build();
    }
  }

  /** Describes a variadic behavior provided by a simple extension. */
  @JsonDeserialize(as = ImmutableSimpleExtension.VariadicBehavior.class)
  @JsonSerialize(as = ImmutableSimpleExtension.VariadicBehavior.class)
  @Value.Immutable
  public interface VariadicBehavior {
    /**
     * Returns the min.
     *
     * @return the min
     */
    int getMin();

    /**
     * Returns the max.
     *
     * @return the max
     */
    OptionalInt getMax();

    /** Enumerates the supported parameter consistency values. */
    enum ParameterConsistency {
      /** The CONSISTENT value. */
      CONSISTENT,
      /** The INCONSISTENT value. */
      INCONSISTENT
    }

    /**
     * Returns the parameter Consistency.
     *
     * @return the parameter Consistency
     */
    @Value.Default
    default ParameterConsistency parameterConsistency() {
      return ParameterConsistency.CONSISTENT;
    }
  }

  /** Describes a function provided by a simple extension. */
  public abstract static class Function {
    private final Supplier<FunctionAnchor> anchorSupplier =
        Util.memoize(() -> FunctionAnchor.of(urn(), key()));
    private final Supplier<String> keySupplier = Util.memoize(() -> constructKey(name(), args()));
    private final Supplier<List<Argument>> requiredArgsSupplier =
        Util.memoize(
            () -> {
              return args().stream().filter(Argument::required).collect(Collectors.toList());
            });

    /**
     * Returns the name.
     *
     * @return the name
     */
    @Value.Default
    public String name() {
      // we can't use null detection here since we initially construct this with a parent name.
      return "";
    }

    /**
     * Returns the urn.
     *
     * @return the urn
     */
    @Value.Default
    public String urn() {
      // we can't use null detection here since we initially construct this without a urn, then
      // resolve later.
      return "";
    }

    /**
     * Returns the variadic.
     *
     * @return the variadic
     */
    public abstract Optional<VariadicBehavior> variadic();

    /**
     * Returns the description.
     *
     * @return the description
     */
    @Value.Default
    @Nullable
    public String description() {
      return "";
    }

    /**
     * Resolves the effective description for this implementation. An implementation-level
     * description (parsed into {@link #description()}) documents overload-specific behavior and
     * takes precedence over the parent function's description when present.
     */
    String resolveDescription(String functionDescription) {
      String implDescription = description();
      return implDescription == null || implDescription.isEmpty()
          ? functionDescription
          : implDescription;
    }

    /**
     * Returns the args.
     *
     * @return the args
     */
    public abstract List<Argument> args();

    /**
     * Returns the options.
     *
     * @return the options
     */
    public abstract Map<String, Option> options();

    /**
     * Returns the metadata.
     *
     * @return the metadata
     */
    public abstract Optional<Map<String, Object>> metadata();

    /**
     * Returns the deprecated.
     *
     * @return the deprecated
     */
    public abstract Optional<DeprecationStatus> deprecated();

    /**
     * Returns the required Arguments.
     *
     * @return the required Arguments
     */
    public List<Argument> requiredArguments() {
      return requiredArgsSupplier.get();
    }

    @Override
    public String toString() {
      return key();
    }

    /**
     * Returns the nullability.
     *
     * @return the nullability
     */
    @Value.Default
    public Nullability nullability() {
      return Nullability.MIRROR;
    }

    /**
     * Returns the ordered.
     *
     * @return the ordered
     */
    @Nullable
    public abstract Boolean ordered();

    /**
     * Returns the anchor.
     *
     * @return the anchor
     */
    public FunctionAnchor getAnchor() {
      return anchorSupplier.get();
    }

    /**
     * Returns the return Type.
     *
     * @return the return Type
     */
    @JsonProperty(value = "return")
    public abstract TypeExpression returnType();

    /**
     * Returns the construct Key From Types for the given arguments.
     *
     * @param name the name
     * @param arguments the arguments
     * @return the construct Key From Types
     */
    public static String constructKeyFromTypes(
        String name, List<io.substrait.type.Type> arguments) {
      try {
        return name
            + ":"
            + arguments.stream()
                .map(t -> t.accept(ToTypeString.INSTANCE))
                .collect(Collectors.joining("_"));
      } catch (UnsupportedOperationException ex) {
        throw new UnsupportedOperationException(
            String.format("Failure converting types of function %s.", name), ex);
      }
    }

    /**
     * Returns the construct Key for the given arguments.
     *
     * @param name the name
     * @param arguments the arguments
     * @return the construct Key
     */
    public static String constructKey(String name, List<Argument> arguments) {
      try {
        return name
            + ":"
            + arguments.stream().map(Argument::toTypeString).collect(Collectors.joining("_"));
      } catch (UnsupportedOperationException ex) {
        throw new UnsupportedOperationException(
            String.format("Failure converting types of function %s.", name), ex);
      }
    }

    /**
     * Returns the range.
     *
     * @return the range
     */
    public Util.IntRange getRange() {
      // end range is exclusive so add one to size.
      int max =
          variadic()
              .map(
                  t -> {
                    OptionalInt optionalMax = t.getMax();
                    IntStream stream =
                        optionalMax.isPresent()
                            ? IntStream.of(optionalMax.getAsInt())
                            : IntStream.empty();
                    return stream
                        .map(x -> args().size() - 1 + x + 1)
                        .findFirst()
                        .orElse(Integer.MAX_VALUE);
                  })
              .orElse(args().size() + 1);
      int min =
          variadic().map(t -> args().size() - 1 + t.getMin()).orElse(requiredArguments().size());
      return Util.IntRange.of(min, max);
    }

    /**
     * Returns the validate Output Type for the given arguments.
     *
     * @param argumentExpressions the argument Expressions
     * @param outputType the output Type
     */
    public void validateOutputType(
        List<Expression> argumentExpressions, io.substrait.type.Type outputType) {
      // TODO: support advanced output type validation using return expressions, parameters, etc.
      // The code below was too restrictive in the case of nullability conversion.
      return;
      //      boolean makeNullable = nullability() == Nullability.MIRROR &&
      //          argumentExpressions.stream().filter(e ->
      // e.getType().nullable()).findFirst().isPresent();
      //      if (returnType() instanceof Type && !outputType.equals(returnType())) {
      //
      //        throw new IllegalArgumentException(String.format("Output type of %s doesn't match
      // expected output
      // type of %s for %s.", outputType, returnType(), this.key()));
      //      }
    }

    /**
     * Returns the key.
     *
     * @return the key
     */
    public String key() {
      return keySupplier.get();
    }

    /**
     * Returns the resolve Type for the given arguments.
     *
     * @param argumentTypes the argument Types
     * @return the resolve Type
     */
    public io.substrait.type.Type resolveType(List<io.substrait.type.Type> argumentTypes) {
      return TypeExpressionEvaluator.evaluateExpression(returnType(), args(), argumentTypes);
    }
  }

  /** Describes a scalar function provided by a simple extension. */
  @JsonDeserialize(as = ImmutableSimpleExtension.ScalarFunction.class)
  @JsonSerialize(as = ImmutableSimpleExtension.ScalarFunction.class)
  @Value.Immutable
  public abstract static class ScalarFunction {
    /**
     * Returns the name.
     *
     * @return the name
     */
    public abstract String name();

    /**
     * Returns the description.
     *
     * @return the description
     */
    @Nullable
    public abstract String description();

    /**
     * Returns the metadata.
     *
     * @return the metadata
     */
    public abstract Optional<Map<String, Object>> metadata();

    /**
     * Returns the deprecated.
     *
     * @return the deprecated
     */
    public abstract Optional<DeprecationStatus> deprecated();

    /**
     * Returns the impls.
     *
     * @return the impls
     */
    public abstract List<ScalarFunctionVariant> impls();

    /**
     * Returns the resolve for the given arguments.
     *
     * @param urn the urn
     * @return the resolve
     */
    public Stream<ScalarFunctionVariant> resolve(String urn) {
      return impls().stream()
          .map(f -> f.resolve(urn, name(), description(), metadata(), deprecated()));
    }
  }

  /** Describes a scalar function variant provided by a simple extension. */
  @JsonDeserialize(as = ImmutableSimpleExtension.ScalarFunctionVariant.class)
  @JsonSerialize(as = ImmutableSimpleExtension.ScalarFunctionVariant.class)
  @Value.Immutable
  public abstract static class ScalarFunctionVariant extends Function {
    ScalarFunctionVariant resolve(
        String urn,
        String name,
        String description,
        Optional<Map<String, Object>> metadata,
        Optional<DeprecationStatus> deprecated) {
      return ImmutableSimpleExtension.ScalarFunctionVariant.builder()
          .urn(urn)
          .name(name)
          .description(resolveDescription(description))
          .nullability(nullability())
          .args(args())
          .options(options())
          .metadata(metadata)
          .deprecated(deprecated().isPresent() ? deprecated() : deprecated)
          .ordered(ordered())
          .variadic(variadic())
          .returnType(returnType())
          .build();
    }
  }

  /** Describes an aggregate function provided by a simple extension. */
  @JsonDeserialize(as = ImmutableSimpleExtension.AggregateFunction.class)
  @JsonSerialize(as = ImmutableSimpleExtension.AggregateFunction.class)
  @Value.Immutable
  public abstract static class AggregateFunction {
    /**
     * Returns the name.
     *
     * @return the name
     */
    @Nullable
    public abstract String name();

    /**
     * Returns the description.
     *
     * @return the description
     */
    @Nullable
    public abstract String description();

    /**
     * Returns the metadata.
     *
     * @return the metadata
     */
    public abstract Optional<Map<String, Object>> metadata();

    /**
     * Returns the deprecated.
     *
     * @return the deprecated
     */
    public abstract Optional<DeprecationStatus> deprecated();

    /**
     * Returns the impls.
     *
     * @return the impls
     */
    public abstract List<AggregateFunctionVariant> impls();

    /**
     * Returns the resolve for the given arguments.
     *
     * @param urn the urn
     * @return the resolve
     */
    public Stream<AggregateFunctionVariant> resolve(String urn) {
      return impls().stream()
          .map(f -> f.resolve(urn, name(), description(), metadata(), deprecated()));
    }
  }

  /** Describes a window function provided by a simple extension. */
  @JsonDeserialize(as = ImmutableSimpleExtension.WindowFunction.class)
  @JsonSerialize(as = ImmutableSimpleExtension.WindowFunction.class)
  @Value.Immutable
  public abstract static class WindowFunction {
    /**
     * Returns the name.
     *
     * @return the name
     */
    @Nullable
    public abstract String name();

    /**
     * Returns the description.
     *
     * @return the description
     */
    @Nullable
    public abstract String description();

    /**
     * Returns the metadata.
     *
     * @return the metadata
     */
    public abstract Optional<Map<String, Object>> metadata();

    /**
     * Returns the deprecated.
     *
     * @return the deprecated
     */
    public abstract Optional<DeprecationStatus> deprecated();

    /**
     * Returns the impls.
     *
     * @return the impls
     */
    public abstract List<WindowFunctionVariant> impls();

    /**
     * Returns the resolve for the given arguments.
     *
     * @param urn the urn
     * @return the resolve
     */
    public Stream<WindowFunctionVariant> resolve(String urn) {
      return impls().stream()
          .map(f -> f.resolve(urn, name(), description(), metadata(), deprecated()));
    }

    /**
     * Creates a new builder.
     *
     * @return a new builder
     */
    public static ImmutableSimpleExtension.WindowFunction.Builder builder() {
      return ImmutableSimpleExtension.WindowFunction.builder();
    }
  }

  /** Describes an aggregate function variant provided by a simple extension. */
  @JsonDeserialize(as = ImmutableSimpleExtension.AggregateFunctionVariant.class)
  @JsonSerialize(as = ImmutableSimpleExtension.AggregateFunctionVariant.class)
  @Value.Immutable
  public abstract static class AggregateFunctionVariant extends Function {
    /**
     * Returns the decomposability.
     *
     * @return the decomposability
     */
    @Value.Default
    @JsonProperty("decomposable")
    public Decomposability decomposability() {
      return Decomposability.NONE;
    }

    @Override
    public String toString() {
      return super.toString();
    }

    /**
     * Returns the intermediate.
     *
     * @return the intermediate
     */
    @Nullable
    public abstract TypeExpression intermediate();

    AggregateFunctionVariant resolve(
        String urn,
        String name,
        String description,
        Optional<Map<String, Object>> metadata,
        Optional<DeprecationStatus> deprecated) {
      return ImmutableSimpleExtension.AggregateFunctionVariant.builder()
          .urn(urn)
          .name(name)
          .description(resolveDescription(description))
          .nullability(nullability())
          .args(args())
          .options(options())
          .metadata(metadata)
          .deprecated(deprecated().isPresent() ? deprecated() : deprecated)
          .ordered(ordered())
          .variadic(variadic())
          .decomposability(decomposability())
          .intermediate(intermediate())
          .returnType(returnType())
          .build();
    }
  }

  /** Describes a window function variant provided by a simple extension. */
  @JsonDeserialize(as = ImmutableSimpleExtension.WindowFunctionVariant.class)
  @JsonSerialize(as = ImmutableSimpleExtension.WindowFunctionVariant.class)
  @Value.Immutable
  public abstract static class WindowFunctionVariant extends Function {

    /**
     * Returns the decomposability.
     *
     * @return the decomposability
     */
    @Value.Default
    @JsonProperty("decomposable")
    public Decomposability decomposability() {
      return Decomposability.NONE;
    }

    /**
     * Returns the intermediate.
     *
     * @return the intermediate
     */
    @Nullable
    public abstract TypeExpression intermediate();

    /**
     * Returns the window Type.
     *
     * @return the window Type
     */
    @Value.Default
    @JsonProperty("window_type")
    public WindowType windowType() {
      return WindowType.PARTITION;
    }

    @Override
    public String toString() {
      return super.toString();
    }

    WindowFunctionVariant resolve(
        String urn,
        String name,
        String description,
        Optional<Map<String, Object>> metadata,
        Optional<DeprecationStatus> deprecated) {
      return ImmutableSimpleExtension.WindowFunctionVariant.builder()
          .urn(urn)
          .name(name)
          .description(resolveDescription(description))
          .nullability(nullability())
          .args(args())
          .options(options())
          .metadata(metadata)
          .deprecated(deprecated().isPresent() ? deprecated() : deprecated)
          .ordered(ordered())
          .variadic(variadic())
          .decomposability(decomposability())
          .intermediate(intermediate())
          .returnType(returnType())
          .windowType(windowType())
          .build();
    }

    /**
     * Creates a new builder.
     *
     * @return a new builder
     */
    public static ImmutableSimpleExtension.WindowFunctionVariant.Builder builder() {
      return ImmutableSimpleExtension.WindowFunctionVariant.builder();
    }
  }

  /** Describes a type provided by a simple extension. */
  @JsonDeserialize(as = ImmutableSimpleExtension.Type.class)
  @JsonSerialize(as = ImmutableSimpleExtension.Type.class)
  @Value.Immutable
  public abstract static class Type {
    private final Supplier<TypeAnchor> anchorSupplier =
        Util.memoize(() -> TypeAnchor.of(urn(), name()));

    /**
     * Returns the name.
     *
     * @return the name
     */
    public abstract String name();

    /**
     * Returns the description.
     *
     * @return the description
     */
    public abstract Optional<String> description();

    /**
     * Returns the urn.
     *
     * @return the urn
     */
    @JacksonInject(SimpleExtension.URN_LOCATOR_KEY)
    public abstract String urn();

    // TODO: Handle conversion of structure object to Named Struct representation
    /**
     * Returns the structure.
     *
     * @return the structure
     */
    protected abstract Optional<Object> structure();

    // TODO: Properly handle parameters
    /**
     * Returns the parameters.
     *
     * @return the parameters
     */
    protected abstract Optional<List<Object>> parameters();

    /**
     * Returns the variadic.
     *
     * @return the variadic
     */
    protected abstract Optional<Boolean> variadic();

    /**
     * Returns the metadata.
     *
     * @return the metadata
     */
    public abstract Optional<Map<String, Object>> metadata();

    /**
     * Returns the deprecated.
     *
     * @return the deprecated
     */
    public abstract Optional<DeprecationStatus> deprecated();

    /**
     * Returns the anchor.
     *
     * @return the anchor
     */
    public TypeAnchor getAnchor() {
      return anchorSupplier.get();
    }
  }

  /** Describes an extension signatures provided by a simple extension. */
  @JsonDeserialize(as = ImmutableSimpleExtension.ExtensionSignatures.class)
  @JsonSerialize(as = ImmutableSimpleExtension.ExtensionSignatures.class)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @Value.Immutable
  public abstract static class ExtensionSignatures {
    /**
     * Returns the types.
     *
     * @return the types
     */
    @JsonProperty("types")
    public abstract List<Type> types();

    /**
     * Returns the urn.
     *
     * @return the urn
     */
    @JsonProperty("urn")
    public abstract String urn();

    /**
     * Returns the scalars.
     *
     * @return the scalars
     */
    @JsonProperty("scalar_functions")
    public abstract List<ScalarFunction> scalars();

    /**
     * Returns the aggregates.
     *
     * @return the aggregates
     */
    @JsonProperty("aggregate_functions")
    public abstract List<AggregateFunction> aggregates();

    /**
     * Returns the windows.
     *
     * @return the windows
     */
    @JsonProperty("window_functions")
    public abstract List<WindowFunction> windows();

    /**
     * Returns the metadata.
     *
     * @return the metadata
     */
    @JsonProperty("metadata")
    public abstract Optional<Map<String, Object>> metadata();

    /**
     * Returns the size.
     *
     * @return the size
     */
    public int size() {
      return (types() == null ? 0 : types().size())
          + (scalars() == null ? 0 : scalars().size())
          + (aggregates() == null ? 0 : aggregates().size())
          + (windows() == null ? 0 : windows().size());
    }

    /**
     * Returns the resolve for the given arguments.
     *
     * @param urn the urn
     * @return the resolve
     */
    public Stream<SimpleExtension.Function> resolve(String urn) {
      return Stream.concat(
          Stream.concat(
              scalars() == null ? Stream.of() : scalars().stream().flatMap(f -> f.resolve(urn)),
              aggregates() == null
                  ? Stream.of()
                  : aggregates().stream().flatMap(f -> f.resolve(urn))),
          windows() == null ? Stream.of() : windows().stream().flatMap(f -> f.resolve(urn)));
    }
  }

  /**
   * The catalog of function and type definitions loaded from YAML extension files. Maps URN + name
   * pairs to full definitions (argument types, return types, etc.).
   *
   * <p>Used by {@link AbstractExtensionLookup#getScalarFunction} and similar methods to resolve a
   * {@link FunctionAnchor} into a complete {@link Function} with signature metadata.
   */
  @Value.Immutable
  public abstract static class ExtensionCollection {
    private final Supplier<Set<String>> urnSupplier =
        Util.memoize(
            () -> {
              return Stream.concat(
                      Stream.concat(
                          scalarFunctions().stream().map(Function::urn),
                          aggregateFunctions().stream().map(Function::urn)),
                      windowFunctions().stream().map(Function::urn))
                  .collect(Collectors.toSet());
            });

    private final Supplier<Map<TypeAnchor, Type>> typeLookup =
        Util.memoize(
            () ->
                types().stream()
                    .collect(
                        Collectors.toMap(Type::getAnchor, java.util.function.Function.identity())));
    private final Supplier<Map<FunctionAnchor, ScalarFunctionVariant>> scalarFunctionsLookup =
        Util.memoize(
            () -> {
              return scalarFunctions().stream()
                  .collect(
                      Collectors.toMap(
                          Function::getAnchor, java.util.function.Function.identity()));
            });

    private final Supplier<Map<FunctionAnchor, AggregateFunctionVariant>> aggregateFunctionsLookup =
        Util.memoize(
            () -> {
              return aggregateFunctions().stream()
                  .collect(
                      Collectors.toMap(
                          Function::getAnchor, java.util.function.Function.identity()));
            });

    private final Supplier<Map<FunctionAnchor, WindowFunctionVariant>> windowFunctionsLookup =
        Util.memoize(
            () -> {
              return windowFunctions().stream()
                  .collect(
                      Collectors.toMap(
                          Function::getAnchor, java.util.function.Function.identity()));
            });

    /**
     * Returns the extension Metadata.
     *
     * @return the extension Metadata
     */
    @Value.Default
    public Map<String, Map<String, Object>> extensionMetadata() {
      return Collections.emptyMap();
    }

    /**
     * Returns the types.
     *
     * @return the types
     */
    public abstract List<Type> types();

    /**
     * Returns the scalar Functions.
     *
     * @return the scalar Functions
     */
    public abstract List<ScalarFunctionVariant> scalarFunctions();

    /**
     * Returns the aggregate Functions.
     *
     * @return the aggregate Functions
     */
    public abstract List<AggregateFunctionVariant> aggregateFunctions();

    /**
     * Returns the window Functions.
     *
     * @return the window Functions
     */
    public abstract List<WindowFunctionVariant> windowFunctions();

    /**
     * Creates a new builder.
     *
     * @return a new builder
     */
    public static ImmutableSimpleExtension.ExtensionCollection.Builder builder() {
      return ImmutableSimpleExtension.ExtensionCollection.builder();
    }

    /**
     * Gets the top-level metadata for a specific extension by URN.
     *
     * @param urn The URN of the extension
     * @return The metadata map if present, empty Optional otherwise
     */
    public Optional<Map<String, Object>> getExtensionMetadata(String urn) {
      return Optional.ofNullable(extensionMetadata().get(urn));
    }

    /**
     * Returns the type for the given arguments.
     *
     * @param anchor the anchor
     * @return the type
     */
    public Type getType(TypeAnchor anchor) {
      Type type = typeLookup.get().get(anchor);
      if (type != null) {
        return type;
      }
      checkUrn(anchor.urn());
      throw new IllegalArgumentException(
          String.format(
              "Unexpected type with name %s. The URN %s is loaded but no type with this name found.",
              anchor.key(), anchor.urn()));
    }

    /**
     * Returns the scalar Function for the given arguments.
     *
     * @param anchor the anchor
     * @return the scalar Function
     */
    public ScalarFunctionVariant getScalarFunction(FunctionAnchor anchor) {
      ScalarFunctionVariant variant = scalarFunctionsLookup.get().get(anchor);
      if (variant != null) {
        return variant;
      }
      checkUrn(anchor.urn());
      throw new IllegalArgumentException(
          String.format(
              "Unexpected scalar function with key %s. The URN %s is loaded "
                  + "but no scalar function with this key found.",
              anchor.key(), anchor.urn()));
    }

    /** Returns true if the given URN has any functions or types loaded in this collection. */
    /**
     * Returns the contains Urn for the given arguments.
     *
     * @param urn the urn
     * @return the contains Urn
     */
    public boolean containsUrn(String urn) {
      return urnSupplier.get().contains(urn) || types().stream().anyMatch(t -> t.urn().equals(urn));
    }

    private void checkUrn(String name) {
      if (urnSupplier.get().contains(name)) {
        return;
      }

      throw new IllegalArgumentException(
          String.format(
              "Received a reference for extension %s "
                  + "but that extension is not currently loaded.",
              name));
    }

    /**
     * Returns the aggregate Function for the given arguments.
     *
     * @param anchor the anchor
     * @return the aggregate Function
     */
    public AggregateFunctionVariant getAggregateFunction(FunctionAnchor anchor) {
      AggregateFunctionVariant variant = aggregateFunctionsLookup.get().get(anchor);
      if (variant != null) {
        return variant;
      }

      checkUrn(anchor.urn());
      throw new IllegalArgumentException(
          String.format(
              "Unexpected aggregate function with key %s. The URN %s is loaded "
                  + "but no aggregate function with this key was found.",
              anchor.key(), anchor.urn()));
    }

    /**
     * Returns the window Function for the given arguments.
     *
     * @param anchor the anchor
     * @return the window Function
     */
    public WindowFunctionVariant getWindowFunction(FunctionAnchor anchor) {
      WindowFunctionVariant variant = windowFunctionsLookup.get().get(anchor);
      if (variant != null) {
        return variant;
      }
      checkUrn(anchor.urn());
      throw new IllegalArgumentException(
          String.format(
              "Unexpected window aggregate function with key %s. The URN %s is loaded "
                  + "but no window aggregate function with this key was found.",
              anchor.key(), anchor.urn()));
    }

    /**
     * Creates the corresponding merge instance.
     *
     * @param extensionCollection the extension Collection
     * @return the merge
     */
    public ExtensionCollection merge(ExtensionCollection extensionCollection) {

      Map<String, Map<String, Object>> mergedExtensionMetadata = new HashMap<>();
      mergedExtensionMetadata.putAll(extensionMetadata());
      mergedExtensionMetadata.putAll(extensionCollection.extensionMetadata());

      return ImmutableSimpleExtension.ExtensionCollection.builder()
          .addAllAggregateFunctions(aggregateFunctions())
          .addAllAggregateFunctions(extensionCollection.aggregateFunctions())
          .addAllScalarFunctions(scalarFunctions())
          .addAllScalarFunctions(extensionCollection.scalarFunctions())
          .addAllWindowFunctions(windowFunctions())
          .addAllWindowFunctions(extensionCollection.windowFunctions())
          .addAllTypes(types())
          .addAllTypes(extensionCollection.types())
          .extensionMetadata(mergedExtensionMetadata)
          .build();
    }
  }

  /**
   * Creates the corresponding load instance.
   *
   * @param resourcePaths the resource Paths
   * @return the load
   */
  public static ExtensionCollection load(List<String> resourcePaths) {
    if (resourcePaths.isEmpty()) {
      throw new IllegalArgumentException("Require at least one resource path.");
    }

    List<ExtensionCollection> extensions =
        resourcePaths.stream()
            .map(
                path -> {
                  try (InputStream stream = ExtensionCollection.class.getResourceAsStream(path)) {
                    return load(stream);
                  } catch (IOException e) {
                    throw new UncheckedIOException(e);
                  }
                })
            .collect(Collectors.toList());
    ExtensionCollection complete = extensions.get(0);
    for (int i = 1; i < extensions.size(); i++) {
      complete = complete.merge(extensions.get(i));
    }
    return complete;
  }

  /**
   * Creates the corresponding load instance.
   *
   * @param content the content
   * @return the load
   */
  public static ExtensionCollection load(String content) {
    try {
      // Parse with basic YAML mapper first to extract URN
      ObjectMapper basicYamlMapper = new ObjectMapper(new YAMLFactory());
      com.fasterxml.jackson.databind.JsonNode rootNode = basicYamlMapper.readTree(content);
      com.fasterxml.jackson.databind.JsonNode urnNode = rootNode.get("urn");
      if (urnNode == null) {
        throw new IllegalArgumentException("Extension YAML file must contain a 'urn' field");
      }
      String urn = urnNode.asText();
      validateUrn(urn);

      ExtensionSignatures doc = objectMapper(urn).readValue(content, ExtensionSignatures.class);

      return buildExtensionCollection(doc);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Creates the corresponding load instance.
   *
   * @param stream the stream
   * @return the load
   */
  public static ExtensionCollection load(InputStream stream) {
    try (Scanner scanner = new Scanner(stream)) {
      scanner.useDelimiter(READ_WHOLE_FILE);
      String content = scanner.next();
      return load(content);
    }
  }

  /**
   * Creates the corresponding build Extension Collection instance.
   *
   * @param extensionSignatures the extension Signatures
   * @return the build Extension Collection
   */
  public static ExtensionCollection buildExtensionCollection(
      ExtensionSignatures extensionSignatures) {
    String urn = extensionSignatures.urn();
    validateUrn(urn);
    List<ScalarFunctionVariant> scalarFunctionVariants =
        extensionSignatures.scalars().stream()
            .flatMap(t -> t.resolve(urn))
            .collect(Collectors.toList());

    List<AggregateFunctionVariant> aggregateFunctionVariants =
        extensionSignatures.aggregates().stream()
            .flatMap(t -> t.resolve(urn))
            .collect(Collectors.toList());

    Stream<WindowFunctionVariant> windowFunctionVariants =
        extensionSignatures.windows().stream().flatMap(t -> t.resolve(urn));

    // Aggregate functions can be used as Window Functions
    Stream<WindowFunctionVariant> windowAggFunctionVariants =
        aggregateFunctionVariants.stream()
            .map(
                afi ->
                    WindowFunctionVariant.builder()
                        // Sets all fields declared in the Function interface
                        .from(afi)
                        // Set WindowFunctionVariant fields
                        .decomposability(afi.decomposability())
                        .intermediate(afi.intermediate())
                        // Aggregate Functions used in Windows have WindowType Streaming
                        .windowType(SimpleExtension.WindowType.STREAMING)
                        .build());

    List<WindowFunctionVariant> allWindowFunctionVariants =
        Stream.concat(windowFunctionVariants, windowAggFunctionVariants)
            .collect(Collectors.toList());

    Map<String, Map<String, Object>> extMetadata = new HashMap<>();
    extensionSignatures.metadata().ifPresent(m -> extMetadata.put(urn, m));

    ImmutableSimpleExtension.ExtensionCollection collection =
        ImmutableSimpleExtension.ExtensionCollection.builder()
            .scalarFunctions(scalarFunctionVariants)
            .aggregateFunctions(aggregateFunctionVariants)
            .windowFunctions(allWindowFunctionVariants)
            .addAllTypes(extensionSignatures.types())
            .extensionMetadata(extMetadata)
            .build();

    LOGGER.atDebug().log(
        "Loaded {} aggregate functions and {} scalar functions from {}.",
        collection.aggregateFunctions().size(),
        collection.scalarFunctions().size(),
        extensionSignatures.urn());
    return collection;
  }
}
