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
  public static final String URN_LOCATOR_KEY = "urn";

  private static final Predicate<String> URN_CHECKER =
      Pattern.compile("^extension:[^:]+:[^:]+$").asPredicate();

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

  public enum Nullability {
    MIRROR,
    DECLARED_OUTPUT,
    DISCRETE
  }

  public enum Decomposability {
    NONE,
    ONE,
    MANY
  }

  public enum WindowType {
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

    @JsonProperty()
    @Nullable String name();

    @JsonProperty()
    @Nullable String description();

    boolean required();
  }

  @JsonDeserialize(as = ImmutableSimpleExtension.Option.class)
  @JsonSerialize(as = ImmutableSimpleExtension.Option.class)
  @Value.Immutable
  public interface Option {
    Optional<String> getDescription();

    List<String> getValues();
  }

  @JsonSerialize(as = ImmutableSimpleExtension.ValueArgument.class)
  @JsonDeserialize(as = ImmutableSimpleExtension.ValueArgument.class)
  @Value.Immutable
  public abstract static class ValueArgument implements Argument {

    @JsonProperty(required = true)
    public abstract ParameterizedType value();

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

    public static ImmutableSimpleExtension.ValueArgument.Builder builder() {
      return ImmutableSimpleExtension.ValueArgument.builder();
    }
  }

  @JsonSerialize(as = ImmutableSimpleExtension.TypeArgument.class)
  @JsonDeserialize(as = ImmutableSimpleExtension.TypeArgument.class)
  @Value.Immutable
  public abstract static class TypeArgument implements Argument {

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

    public static ImmutableSimpleExtension.EnumArgument.Builder builder() {
      return ImmutableSimpleExtension.EnumArgument.builder();
    }
  }

  public interface Anchor {
    String urn();

    String key();
  }

  @Value.Immutable
  public interface FunctionAnchor extends Anchor {
    static FunctionAnchor of(String urn, String key) {
      return ImmutableSimpleExtension.FunctionAnchor.builder().urn(urn).key(key).build();
    }
  }

  @Value.Immutable
  public interface TypeAnchor extends Anchor {
    static TypeAnchor of(String urn, String name) {
      return ImmutableSimpleExtension.TypeAnchor.builder().urn(urn).key(name).build();
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
    private final Supplier<FunctionAnchor> anchorSupplier =
        Util.memoize(() -> FunctionAnchor.of(urn(), key()));
    private final Supplier<String> keySupplier = Util.memoize(() -> constructKey(name(), args()));
    private final Supplier<List<Argument>> requiredArgsSupplier =
        Util.memoize(
            () -> {
              return args().stream().filter(Argument::required).collect(Collectors.toList());
            });

    @Value.Default
    public String name() {
      // we can't use null detection here since we initially construct this with a parent name.
      return "";
    }

    @Value.Default
    public String urn() {
      // we can't use null detection here since we initially construct this without a urn, then
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

    public String key() {
      return keySupplier.get();
    }

    public io.substrait.type.Type resolveType(List<io.substrait.type.Type> argumentTypes) {
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

    public Stream<ScalarFunctionVariant> resolve(String urn) {
      return impls().stream().map(f -> f.resolve(urn, name(), description()));
    }
  }

  @JsonDeserialize(as = ImmutableSimpleExtension.ScalarFunctionVariant.class)
  @JsonSerialize(as = ImmutableSimpleExtension.ScalarFunctionVariant.class)
  @Value.Immutable
  public abstract static class ScalarFunctionVariant extends Function {
    public ScalarFunctionVariant resolve(String urn, String name, String description) {
      return ImmutableSimpleExtension.ScalarFunctionVariant.builder()
          .urn(urn)
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

    public Stream<AggregateFunctionVariant> resolve(String urn) {
      return impls().stream().map(f -> f.resolve(urn, name(), description()));
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

    public Stream<WindowFunctionVariant> resolve(String urn) {
      return impls().stream().map(f -> f.resolve(urn, name(), description()));
    }

    public static ImmutableSimpleExtension.WindowFunction.Builder builder() {
      return ImmutableSimpleExtension.WindowFunction.builder();
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

    AggregateFunctionVariant resolve(String urn, String name, String description) {
      return ImmutableSimpleExtension.AggregateFunctionVariant.builder()
          .urn(urn)
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

    WindowFunctionVariant resolve(String urn, String name, String description) {
      return ImmutableSimpleExtension.WindowFunctionVariant.builder()
          .urn(urn)
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

    public static ImmutableSimpleExtension.WindowFunctionVariant.Builder builder() {
      return ImmutableSimpleExtension.WindowFunctionVariant.builder();
    }
  }

  @JsonDeserialize(as = ImmutableSimpleExtension.Type.class)
  @JsonSerialize(as = ImmutableSimpleExtension.Type.class)
  @Value.Immutable
  public abstract static class Type {
    private final Supplier<TypeAnchor> anchorSupplier =
        Util.memoize(() -> TypeAnchor.of(urn(), name()));

    public abstract String name();

    @JacksonInject(SimpleExtension.URN_LOCATOR_KEY)
    public abstract String urn();

    // TODO: Handle conversion of structure object to Named Struct representation
    protected abstract Optional<Object> structure();

    public TypeAnchor getAnchor() {
      return anchorSupplier.get();
    }
  }

  @JsonDeserialize(as = ImmutableSimpleExtension.ExtensionSignatures.class)
  @JsonSerialize(as = ImmutableSimpleExtension.ExtensionSignatures.class)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @Value.Immutable
  public abstract static class ExtensionSignatures {
    @JsonProperty("types")
    public abstract List<Type> types();

    @JsonProperty("urn")
    public abstract String urn();

    // URI is not from YAML, but from the loading context
    // this only needs to be present temporarily to handle the URI -> URN migration
    @Value.Default
    public String uri() {
      return "";
    }

    @JsonProperty("scalar_functions")
    public abstract List<ScalarFunction> scalars();

    @JsonProperty("aggregate_functions")
    public abstract List<AggregateFunction> aggregates();

    @JsonProperty("window_functions")
    public abstract List<WindowFunction> windows();

    public int size() {
      return (types() == null ? 0 : types().size())
          + (scalars() == null ? 0 : scalars().size())
          + (aggregates() == null ? 0 : aggregates().size())
          + (windows() == null ? 0 : windows().size());
    }

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

    @Value.Default
    BidiMap<String, String> uriUrnMap() {
      return new BidiMap<>();
    }

    public abstract List<Type> types();

    public abstract List<ScalarFunctionVariant> scalarFunctions();

    public abstract List<AggregateFunctionVariant> aggregateFunctions();

    public abstract List<WindowFunctionVariant> windowFunctions();

    public static ImmutableSimpleExtension.ExtensionCollection.Builder builder() {
      return ImmutableSimpleExtension.ExtensionCollection.builder();
    }

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
     * Gets the URI for a given URN. This is for internal framework use during URI/URN migration.
     *
     * @param urn The URN to look up
     * @return The corresponding URI, or null if not found
     */
    public String getUriFromUrn(String urn) {
      return uriUrnMap().reverseGet(urn);
    }

    /**
     * Gets the URN for a given URI. This is for internal framework use during URI/URN migration.
     *
     * @param uri The URI to look up
     * @return The corresponding URN, or null if not found
     */
    public String getUrnFromUri(String uri) {
      return uriUrnMap().get(uri);
    }

    public ExtensionCollection merge(ExtensionCollection extensionCollection) {
      BidiMap<String, String> mergedUriUrnMap = new BidiMap<>();
      mergedUriUrnMap.merge(uriUrnMap());
      mergedUriUrnMap.merge(extensionCollection.uriUrnMap());

      return ImmutableSimpleExtension.ExtensionCollection.builder()
          .addAllAggregateFunctions(aggregateFunctions())
          .addAllAggregateFunctions(extensionCollection.aggregateFunctions())
          .addAllScalarFunctions(scalarFunctions())
          .addAllScalarFunctions(extensionCollection.scalarFunctions())
          .addAllWindowFunctions(windowFunctions())
          .addAllWindowFunctions(extensionCollection.windowFunctions())
          .addAllTypes(types())
          .addAllTypes(extensionCollection.types())
          .uriUrnMap(mergedUriUrnMap)
          .build();
    }
  }

  public static ExtensionCollection load(List<String> resourcePaths) {
    if (resourcePaths.isEmpty()) {
      throw new IllegalArgumentException("Require at least one resource path.");
    }

    List<ExtensionCollection> extensions =
        resourcePaths.stream()
            .map(
                path -> {
                  try (InputStream stream = ExtensionCollection.class.getResourceAsStream(path)) {
                    return load(path, stream);
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

  public static ExtensionCollection load(String uri, String content) {
    try {
      if (uri == null || uri.isEmpty()) {
        throw new IllegalArgumentException("URI cannot be null or empty");
      }

      // Parse with basic YAML mapper first to extract URN
      ObjectMapper basicYamlMapper = new ObjectMapper(new YAMLFactory());
      com.fasterxml.jackson.databind.JsonNode rootNode = basicYamlMapper.readTree(content);
      com.fasterxml.jackson.databind.JsonNode urnNode = rootNode.get("urn");
      if (urnNode == null) {
        throw new IllegalArgumentException("Extension YAML file must contain a 'urn' field");
      }
      String urn = urnNode.asText();
      validateUrn(urn);

      ExtensionSignatures docWithoutUri =
          objectMapper(urn).readValue(content, ExtensionSignatures.class);

      ExtensionSignatures doc =
          ImmutableSimpleExtension.ExtensionSignatures.builder()
              .from(docWithoutUri)
              .uri(uri)
              .build();

      return buildExtensionCollection(uri, doc);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public static ExtensionCollection load(String uri, InputStream stream) {
    try (Scanner scanner = new Scanner(stream)) {
      scanner.useDelimiter("\\A");
      String content = scanner.next();
      return load(uri, content);
    }
  }

  public static ExtensionCollection buildExtensionCollection(
      String uri, ExtensionSignatures extensionSignatures) {
    String urn = extensionSignatures.urn();
    validateUrn(urn);
    if (uri == null || uri == "") {
      throw new IllegalArgumentException("URI cannot be null or empty");
    }
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

    BidiMap<String, String> uriUrnMap = new BidiMap<>();
    uriUrnMap.put(uri, urn);

    ImmutableSimpleExtension.ExtensionCollection collection =
        ImmutableSimpleExtension.ExtensionCollection.builder()
            .scalarFunctions(scalarFunctionVariants)
            .aggregateFunctions(aggregateFunctionVariants)
            .windowFunctions(allWindowFunctionVariants)
            .addAllTypes(extensionSignatures.types())
            .uriUrnMap(uriUrnMap)
            .build();

    LOGGER.atDebug().log(
        "Loaded {} aggregate functions and {} scalar functions from {}.",
        collection.aggregateFunctions().size(),
        collection.scalarFunctions().size(),
        extensionSignatures.urn());
    return collection;
  }
}
