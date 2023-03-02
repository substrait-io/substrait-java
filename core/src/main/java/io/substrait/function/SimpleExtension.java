package io.substrait.function;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.substrait.expression.Expression;
import io.substrait.type.Deserializers;
import io.substrait.type.Type;
import io.substrait.type.TypeExpressionEvaluator;
import io.substrait.util.Util;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * Classes used to deserialize YAML extension files. Currently, constrained to Function
 * deserialization.
 */
@Value.Enclosing
public class SimpleExtension {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleExtension.class);

  private static final ObjectMapper MAPPER =
      new ObjectMapper(new YAMLFactory())
          .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
          .registerModule(new Jdk8Module())
          .registerModule(Deserializers.MODULE);

  enum Nullability {
    MIRROR,
    DECLARED_OUTPUT,
    DISCRETE
  }

  enum Decomposability {
    NONE,
    ONE,
    MANY
  }

  enum WindowType {
    PARTITION,
    STREAMING
  }

  private SimpleExtension() {}

  @JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION)
  @JsonSubTypes({
    @JsonSubTypes.Type(ValueArgument.class),
    @JsonSubTypes.Type(TypeArgument.class),
    @JsonSubTypes.Type(EnumArgument.class)
  })
  public interface Argument {
    String toTypeString();

    @Nullable
    String description();

    boolean required();
  }

  @JsonDeserialize(as = ImmutableSimpleExtension.Option.class)
  @JsonSerialize(as = ImmutableSimpleExtension.Option.class)
  @Value.Immutable
  public interface Option {
    Optional<String> getDescription();

    List<String> getValues();
  }

  public static class ValueArgument implements Argument {

    @JsonCreator
    public ValueArgument(
        @JsonProperty("value") ParameterizedType value,
        @JsonProperty("name") String name,
        @JsonProperty("constant") boolean constant,
        @JsonProperty("description") String description) {
      this.value = value;
      this.constant = constant;
      this.name = name;
      this.description = description;
    }

    @JsonProperty(required = true)
    ParameterizedType value;

    String name;

    boolean constant;

    String description;

    public ParameterizedType value() {
      return value;
    }

    public String description() {
      return description;
    }

    @Override
    public String toTypeString() {
      return value.accept(ToTypeString.INSTANCE);
    }

    public boolean required() {
      return true;
    }
  }

  public static class TypeArgument implements Argument {

    @JsonCreator
    public TypeArgument(
        @JsonProperty("type") ParameterizedType type,
        @JsonProperty("name") String name,
        @JsonProperty("description") String description) {
      this.type = type;
      this.name = name;
      this.description = description;
    }

    private ParameterizedType type;
    private String name;
    private String description;

    public ParameterizedType getType() {
      return type;
    }

    public void setType(final ParameterizedType type) {
      this.type = type;
    }

    public String description() {
      return description;
    }

    public String toTypeString() {
      return "type";
    }

    public boolean required() {
      return true;
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
  public static class EnumArgument implements Argument {

    @JsonCreator
    public EnumArgument(
        @JsonProperty("options") List<String> options,
        @JsonProperty("name") String name,
        @JsonProperty("description") String description) {
      this.options = options;
      this.name = name;
      this.description = description;
    }

    private final List<String> options;
    private final String name;
    private final String description;

    public List<String> options() {
      return options;
    }

    public String description() {
      return description;
    }

    @Override
    public boolean required() {
      return true;
    }

    public String toTypeString() {
      return "req";
    }
  }

  @Value.Immutable
  public interface FunctionAnchor {
    String namespace();

    String key();

    static FunctionAnchor of(String namespace, String key) {
      return ImmutableSimpleExtension.FunctionAnchor.builder()
          .namespace(namespace)
          .key(key)
          .build();
    }
  }

  @JsonDeserialize(as = ImmutableSimpleExtension.VariadicBehavior.class)
  @JsonSerialize(as = ImmutableSimpleExtension.VariadicBehavior.class)
  @Value.Immutable
  public interface VariadicBehavior {
    int getMin();

    OptionalInt getMax();

    enum ParameterConsistency {
      CONSISTENT,
      INCONSISTENT
    }

    default ParameterConsistency parameterConsistency() {
      return ParameterConsistency.CONSISTENT;
    }
  }

  public abstract static class Function {
    @Value.Default
    public String name() {
      // we can't use null detection here since we initially construct this with a parent name.
      return "";
    }

    @Value.Default
    public String uri() {
      // we can't use null detection here since we initially construct this without a uri, then
      // resolve later.
      return "";
    }

    public abstract Optional<VariadicBehavior> variadic();

    @Value.Default
    @Nullable
    public String description() {
      return "";
    }

    public abstract List<Argument> args();

    public abstract Map<String, Option> options();

    public List<Argument> requiredArguments() {
      return requiredArgsSupplier.get();
    }

    @Override
    public String toString() {
      return key();
    }

    @Value.Default
    public Nullability nullability() {
      return Nullability.MIRROR;
    }

    @Nullable
    public abstract Boolean ordered();

    public FunctionAnchor getAnchor() {
      return anchorSupplier.get();
    }

    @JsonProperty(value = "return")
    public abstract TypeExpression returnType();

    private final Supplier<FunctionAnchor> anchorSupplier =
        Util.memoize(() -> FunctionAnchor.of(uri(), key()));
    private final Supplier<String> keySupplier = Util.memoize(() -> constructKey(name(), args()));
    private final Supplier<List<Argument>> requiredArgsSupplier =
        Util.memoize(
            () -> {
              return args().stream()
                  .filter(Argument::required)
                  .collect(java.util.stream.Collectors.toList());
            });

    public static String constructKeyFromTypes(String name, List<Type> arguments) {
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

    public Util.IntRange getRange() {
      // end range is exclusive so add one to size.

      long optionalCount = args().stream().filter(t -> !t.required()).count();
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

    public void validateOutputType(List<Expression> argumentExpressions, Type outputType) {
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

    public String key() {
      return keySupplier.get();
    }

    public Type resolveType(List<Type> argumentTypes) {
      return TypeExpressionEvaluator.evaluateExpression(returnType(), args(), argumentTypes);
    }
  }

  @JsonDeserialize(as = ImmutableSimpleExtension.ScalarFunction.class)
  @JsonSerialize(as = ImmutableSimpleExtension.ScalarFunction.class)
  @Value.Immutable
  public abstract static class ScalarFunction {
    public abstract String name();

    @Nullable
    public abstract String description();

    public abstract List<ScalarFunctionVariant> impls();

    public Stream<ScalarFunctionVariant> resolve(String uri) {
      return impls().stream().map(f -> f.resolve(uri, name(), description()));
    }
  }

  @JsonDeserialize(as = ImmutableSimpleExtension.ScalarFunctionVariant.class)
  @JsonSerialize(as = ImmutableSimpleExtension.ScalarFunctionVariant.class)
  @Value.Immutable
  public abstract static class ScalarFunctionVariant extends Function {
    public ScalarFunctionVariant resolve(String uri, String name, String description) {
      return ImmutableSimpleExtension.ScalarFunctionVariant.builder()
          .uri(uri)
          .name(name)
          .description(description)
          .nullability(nullability())
          .args(args())
          .options(options())
          .ordered(ordered())
          .variadic(variadic())
          .returnType(returnType())
          .build();
    }
  }

  @JsonDeserialize(as = ImmutableSimpleExtension.AggregateFunction.class)
  @JsonSerialize(as = ImmutableSimpleExtension.AggregateFunction.class)
  @Value.Immutable
  public abstract static class AggregateFunction {
    @Nullable
    public abstract String name();

    @Nullable
    public abstract String description();

    public abstract List<AggregateFunctionVariant> impls();

    public Stream<AggregateFunctionVariant> resolve(String uri) {
      return impls().stream().map(f -> f.resolve(uri, name(), description()));
    }
  }

  @JsonDeserialize(as = ImmutableSimpleExtension.WindowFunction.class)
  @JsonSerialize(as = ImmutableSimpleExtension.WindowFunction.class)
  @Value.Immutable
  public abstract static class WindowFunction {
    @Nullable
    public abstract String name();

    @Nullable
    public abstract String description();

    public abstract List<WindowFunctionVariant> impls();

    public Stream<WindowFunctionVariant> resolve(String uri) {
      return impls().stream().map(f -> f.resolve(uri, name(), description()));
    }
  }

  @JsonDeserialize(as = ImmutableSimpleExtension.AggregateFunctionVariant.class)
  @JsonSerialize(as = ImmutableSimpleExtension.AggregateFunctionVariant.class)
  @Value.Immutable
  public abstract static class AggregateFunctionVariant extends Function {
    @Value.Default
    @JsonProperty("decomposable")
    public Decomposability decomposability() {
      return Decomposability.NONE;
    }

    @Override
    public String toString() {
      return super.toString();
    }

    @Nullable
    public abstract TypeExpression intermediate();

    AggregateFunctionVariant resolve(String uri, String name, String description) {
      return ImmutableSimpleExtension.AggregateFunctionVariant.builder()
          .uri(uri)
          .name(name)
          .description(description)
          .nullability(nullability())
          .args(args())
          .options(options())
          .ordered(ordered())
          .variadic(variadic())
          .decomposability(decomposability())
          .intermediate(intermediate())
          .returnType(returnType())
          .build();
    }
  }

  @JsonDeserialize(as = ImmutableSimpleExtension.WindowFunctionVariant.class)
  @JsonSerialize(as = ImmutableSimpleExtension.WindowFunctionVariant.class)
  @Value.Immutable
  public abstract static class WindowFunctionVariant extends Function {

    @Value.Default
    @JsonProperty("decomposable")
    public Decomposability decomposability() {
      return Decomposability.NONE;
    }

    @Nullable
    public abstract TypeExpression intermediate();

    @Value.Default
    @JsonProperty("window_type")
    public WindowType windowType() {
      return WindowType.PARTITION;
    }

    @Override
    public String toString() {
      return super.toString();
    }

    WindowFunctionVariant resolve(String uri, String name, String description) {
      return ImmutableSimpleExtension.WindowFunctionVariant.builder()
          .uri(uri)
          .name(name)
          .description(description)
          .nullability(nullability())
          .args(args())
          .options(options())
          .ordered(ordered())
          .variadic(variadic())
          .decomposability(decomposability())
          .intermediate(intermediate())
          .returnType(returnType())
          .windowType(windowType())
          .build();
    }
  }

  @JsonDeserialize(as = ImmutableSimpleExtension.FunctionSignatures.class)
  @JsonSerialize(as = ImmutableSimpleExtension.FunctionSignatures.class)
  @Value.Immutable
  public abstract static class FunctionSignatures {
    @JsonProperty("scalar_functions")
    public abstract List<ScalarFunction> scalars();

    @JsonProperty("aggregate_functions")
    public abstract List<AggregateFunction> aggregates();

    @JsonProperty("window_functions")
    public abstract List<WindowFunction> windows();

    public int size() {
      return (scalars() == null ? 0 : scalars().size())
          + (aggregates() == null ? 0 : aggregates().size())
          + (windows() == null ? 0 : windows().size());
    }

    public Stream<SimpleExtension.Function> resolve(String uri) {
      return Stream.concat(
          Stream.concat(
              scalars() == null ? Stream.of() : scalars().stream().flatMap(f -> f.resolve(uri)),
              aggregates() == null
                  ? Stream.of()
                  : aggregates().stream().flatMap(f -> f.resolve(uri))),
          windows() == null ? Stream.of() : windows().stream().flatMap(f -> f.resolve(uri)));
    }
  }

  @Value.Immutable
  public abstract static class ExtensionCollection {
    public abstract List<ScalarFunctionVariant> scalarFunctions();

    public abstract List<AggregateFunctionVariant> aggregateFunctions();

    public abstract List<WindowFunctionVariant> windowFunctions();

    private final Supplier<Set<String>> namespaceSupplier =
        Util.memoize(
            () -> {
              return Stream.concat(
                      Stream.concat(
                          scalarFunctions().stream().map(Function::uri),
                          aggregateFunctions().stream().map(Function::uri)),
                      windowFunctions().stream().map(Function::uri))
                  .collect(Collectors.toSet());
            });
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

    public ScalarFunctionVariant getScalarFunction(FunctionAnchor anchor) {
      ScalarFunctionVariant variant = scalarFunctionsLookup.get().get(anchor);
      if (variant != null) {
        return variant;
      }
      checkNamespace(anchor.namespace());
      throw new IllegalArgumentException(
          String.format(
              "Unexpected scalar function with key %s. The namespace %s is loaded "
                  + "but no scalar function with this key found.",
              anchor.key(), anchor.namespace()));
    }

    private void checkNamespace(String name) {
      if (namespaceSupplier.get().contains(name)) {
        return;
      }

      throw new IllegalArgumentException(
          String.format(
              "Received a reference for extension %s "
                  + "but that extension is not currently loaded.",
              name));
    }

    public AggregateFunctionVariant getAggregateFunction(FunctionAnchor anchor) {
      var variant = aggregateFunctionsLookup.get().get(anchor);
      if (variant != null) {
        return variant;
      }

      checkNamespace(anchor.namespace());
      throw new IllegalArgumentException(
          String.format(
              "Unexpected aggregate function with key %s. The namespace %s is loaded "
                  + "but no aggregate function with this key was found.",
              anchor.key(), anchor.namespace()));
    }

    public WindowFunctionVariant getWindowFunction(FunctionAnchor anchor) {
      var variant = windowFunctionsLookup.get().get(anchor);
      if (variant != null) {
        return variant;
      }
      checkNamespace(anchor.namespace());
      throw new IllegalArgumentException(
          String.format(
              "Unexpected window aggregate function with key %s. The namespace %s is loaded "
                  + "but no window aggregate function with this key was found.",
              anchor.key(), anchor.namespace()));
    }

    public ExtensionCollection merge(ExtensionCollection extensionCollection) {
      return ImmutableSimpleExtension.ExtensionCollection.builder()
          .addAllAggregateFunctions(aggregateFunctions())
          .addAllAggregateFunctions(extensionCollection.aggregateFunctions())
          .addAllScalarFunctions(scalarFunctions())
          .addAllScalarFunctions(extensionCollection.scalarFunctions())
          .addAllWindowFunctions(windowFunctions())
          .addAllWindowFunctions(extensionCollection.windowFunctions())
          .build();
    }
  }

  public static ExtensionCollection loadDefaults() throws IOException {
    var defaultFiles =
        Arrays.asList(
                "boolean",
                "aggregate_generic",
                "aggregate_approx",
                "arithmetic_decimal",
                "arithmetic",
                "comparison",
                "datetime",
                "string")
            .stream()
            .map(c -> String.format("/functions_%s.yaml", c))
            .collect(java.util.stream.Collectors.toList());

    return load(defaultFiles);
  }

  public static ExtensionCollection load(List<String> resourcePaths) throws IOException {
    if (resourcePaths.isEmpty()) {
      throw new IllegalArgumentException("Require at least one resource path.");
    }

    var extensions =
        resourcePaths.stream()
            .map(
                path -> {
                  try (var stream = ExtensionCollection.class.getResourceAsStream(path)) {
                    return load(path, stream);
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(java.util.stream.Collectors.toList());
    ExtensionCollection complete = extensions.get(0);
    for (int i = 1; i < extensions.size(); i++) {
      complete = complete.merge(extensions.get(i));
    }
    return complete;
  }

  private static ExtensionCollection load(String namespace, InputStream stream) {
    try {
      var doc = MAPPER.readValue(stream, SimpleExtension.FunctionSignatures.class);
      var collection =
          ImmutableSimpleExtension.ExtensionCollection.builder()
              .addAllAggregateFunctions(
                  doc.aggregates().stream()
                      .flatMap(t -> t.resolve(namespace))
                      .collect(java.util.stream.Collectors.toList()))
              .addAllScalarFunctions(
                  doc.scalars().stream()
                      .flatMap(t -> t.resolve(namespace))
                      .collect(java.util.stream.Collectors.toList()))
              .addAllWindowFunctions(
                  doc.windows().stream()
                      .flatMap(t -> t.resolve(namespace))
                      .collect(java.util.stream.Collectors.toList()))
              .build();
      logger.debug(
          "Loaded {} aggregate functions and {} scalar functions from {}.",
          collection.aggregateFunctions().size(),
          collection.scalarFunctions().size(),
          namespace);
      return collection;
    } catch (RuntimeException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new RuntimeException("Failure while parsing " + namespace, ex);
    }
  }
}
