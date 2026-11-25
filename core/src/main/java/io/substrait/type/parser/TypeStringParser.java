package io.substrait.type.parser;

import io.substrait.antlr.SubstraitTypeLexer;
import io.substrait.antlr.SubstraitTypeParser;
import io.substrait.function.ParameterizedType;
import io.substrait.function.ParameterizedTypeCreator;
import io.substrait.function.TypeExpression;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

/** Parses Substrait type strings into their {@link TypeExpression} representations. */
public class TypeStringParser {

  private static final int MAX_CACHE_SIZE = 2048;

  private static final Map<String, Type> SIMPLE_FAST_PATH = buildSimpleFastPath();

  private static final Map<String, ParameterizedType> PARAMETERIZED_FAST_PATH =
      buildParameterizedFastPath();

  private static final ConcurrentHashMap<String, Type> SIMPLE_CACHE = new ConcurrentHashMap<>();

  private static final ConcurrentHashMap<String, ParameterizedType> PARAMETERIZED_CACHE =
      new ConcurrentHashMap<>();

  private static final ConcurrentHashMap<String, TypeExpression> EXPRESSION_CACHE =
      new ConcurrentHashMap<>();

  private TypeStringParser() {}

  private static Map<String, Type> buildSimpleFastPath() {
    Map<String, Type> map = new HashMap<>(64);
    registerScalar(map, "boolean", TypeCreator.REQUIRED.BOOLEAN, TypeCreator.NULLABLE.BOOLEAN);
    registerScalar(map, "i8", TypeCreator.REQUIRED.I8, TypeCreator.NULLABLE.I8);
    registerScalar(map, "i16", TypeCreator.REQUIRED.I16, TypeCreator.NULLABLE.I16);
    registerScalar(map, "i32", TypeCreator.REQUIRED.I32, TypeCreator.NULLABLE.I32);
    registerScalar(map, "i64", TypeCreator.REQUIRED.I64, TypeCreator.NULLABLE.I64);
    registerScalar(map, "fp32", TypeCreator.REQUIRED.FP32, TypeCreator.NULLABLE.FP32);
    registerScalar(map, "fp64", TypeCreator.REQUIRED.FP64, TypeCreator.NULLABLE.FP64);
    registerScalar(map, "string", TypeCreator.REQUIRED.STRING, TypeCreator.NULLABLE.STRING);
    registerScalar(map, "binary", TypeCreator.REQUIRED.BINARY, TypeCreator.NULLABLE.BINARY);
    registerScalar(map, "date", TypeCreator.REQUIRED.DATE, TypeCreator.NULLABLE.DATE);
    registerScalar(
        map,
        "interval_year",
        TypeCreator.REQUIRED.INTERVAL_YEAR,
        TypeCreator.NULLABLE.INTERVAL_YEAR);
    registerScalar(map, "uuid", TypeCreator.REQUIRED.UUID, TypeCreator.NULLABLE.UUID);
    return Collections.unmodifiableMap(map);
  }

  private static void registerScalar(
      Map<String, Type> map, String name, Type required, Type nullable) {
    map.put(name, required);
    map.put(name + "?", nullable);
  }

  private static Map<String, ParameterizedType> buildParameterizedFastPath() {
    Map<String, ParameterizedType> map = new HashMap<>(128);
    map.putAll(SIMPLE_FAST_PATH);
    registerWildcard(map, "any");
    for (int i = 0; i <= 9; i++) {
      registerWildcard(map, "any" + i);
    }
    return Collections.unmodifiableMap(map);
  }

  private static void registerWildcard(Map<String, ParameterizedType> map, String name) {
    map.put(name, ParameterizedTypeCreator.REQUIRED.parameter(name));
    map.put(name + "?", ParameterizedTypeCreator.NULLABLE.parameter(name));
  }

  /**
   * Returns whether {@code str} may contain a user-defined type token ({@code u!} / {@code U!}),
   * whose parsed {@link Type.UserDefined} carries the caller's URN and therefore must not be shared
   * across URNs in the URN-agnostic cache.
   */
  private static boolean containsUserDefinedType(String str) {
    for (int i = str.indexOf('!'); i >= 0; i = str.indexOf('!', i + 1)) {
      if (i > 0) {
        char prev = str.charAt(i - 1);
        if (prev == 'u' || prev == 'U') {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Adds an entry unless the cache is full. Every insertion holds the cache's monitor, so
   * concurrent misses cannot exceed {@link #MAX_CACHE_SIZE}. Reads and parsing do not take the
   * monitor.
   */
  private static <T> void cacheIfBounded(ConcurrentHashMap<String, T> cache, String key, T value) {
    synchronized (cache) {
      if (cache.size() < MAX_CACHE_SIZE) {
        cache.putIfAbsent(key, value);
      }
    }
  }

  /**
   * Parses a string into a simple {@link Type}.
   *
   * @param str the type string to parse
   * @param urn the extension URN used to resolve user-defined types
   * @return the parsed type
   */
  public static Type parseSimple(String str, String urn) {
    if (str == null) {
      return parse(str, urn, ParseToPojo::type);
    }
    Type fast = SIMPLE_FAST_PATH.get(str);
    if (fast != null) {
      return fast;
    }
    if (containsUserDefinedType(str)) {
      return parse(str, urn, ParseToPojo::type);
    }
    Type cached = SIMPLE_CACHE.get(str);
    if (cached != null) {
      return cached;
    }
    Type parsed = parse(str, urn, ParseToPojo::type);
    cacheIfBounded(SIMPLE_CACHE, str, parsed);
    return parsed;
  }

  /**
   * Parses a string into a {@link ParameterizedType}.
   *
   * @param str the type string to parse
   * @param urn the extension URN used to resolve user-defined types
   * @return the parsed parameterized type
   */
  public static ParameterizedType parseParameterized(String str, String urn) {
    if (str == null) {
      return parse(str, urn, ParseToPojo::parameterizedType);
    }
    ParameterizedType fast = PARAMETERIZED_FAST_PATH.get(str);
    if (fast != null) {
      return fast;
    }
    if (containsUserDefinedType(str)) {
      return parse(str, urn, ParseToPojo::parameterizedType);
    }
    ParameterizedType cached = PARAMETERIZED_CACHE.get(str);
    if (cached != null) {
      return cached;
    }
    ParameterizedType parsed = parse(str, urn, ParseToPojo::parameterizedType);
    cacheIfBounded(PARAMETERIZED_CACHE, str, parsed);
    return parsed;
  }

  /**
   * Parses a string into a {@link TypeExpression}.
   *
   * @param str the type string to parse
   * @param urn the extension URN used to resolve user-defined types
   * @return the parsed type expression
   */
  public static TypeExpression parseExpression(String str, String urn) {
    if (str == null) {
      return parse(str, urn, ParseToPojo::typeExpression);
    }
    TypeExpression fast = PARAMETERIZED_FAST_PATH.get(str);
    if (fast != null) {
      return fast;
    }
    if (containsUserDefinedType(str)) {
      return parse(str, urn, ParseToPojo::typeExpression);
    }
    TypeExpression cached = EXPRESSION_CACHE.get(str);
    if (cached != null) {
      return cached;
    }
    TypeExpression parsed = parse(str, urn, ParseToPojo::typeExpression);
    cacheIfBounded(EXPRESSION_CACHE, str, parsed);
    return parsed;
  }

  private static SubstraitTypeParser.StartRuleContext parse(String str) {
    SubstraitTypeLexer lexer = new SubstraitTypeLexer(CharStreams.fromString(str));
    lexer.removeErrorListeners();
    lexer.addErrorListener(TypeErrorListener.INSTANCE);
    CommonTokenStream tokenStream = new CommonTokenStream(lexer);
    SubstraitTypeParser parser = new io.substrait.antlr.SubstraitTypeParser(tokenStream);
    parser.removeErrorListeners();
    parser.addErrorListener(TypeErrorListener.INSTANCE);
    return parser.startRule();
  }

  /**
   * Parses a type string and converts the parse tree using the given function.
   *
   * @param str the type string to parse
   * @param urn the extension URN used to resolve user-defined types
   * @param func function converting the parse tree to the desired result
   * @param <T> the result type
   * @return the converted result
   */
  public static <T> T parse(
      String str, String urn, BiFunction<String, SubstraitTypeParser.StartRuleContext, T> func) {
    return func.apply(urn, parse(str));
  }

  /**
   * Parses a type string and converts the parse tree using the given visitor.
   *
   * @param str the type string to parse
   * @param visitor the visitor converting the parse tree
   * @return the parsed type expression
   */
  public static TypeExpression parse(String str, ParseToPojo.Visitor visitor) {
    if (str == null || visitor == null) {
      return parse(str).accept(visitor);
    }
    switch (visitor.getVisitorType()) {
      case SIMPLE:
        Type fastSimple = SIMPLE_FAST_PATH.get(str);
        if (fastSimple != null) {
          return fastSimple;
        }
        if (containsUserDefinedType(str)) {
          return parse(str).accept(visitor);
        }
        Type cachedSimple = SIMPLE_CACHE.get(str);
        if (cachedSimple != null) {
          return cachedSimple;
        }
        TypeExpression parsedSimple = parse(str).accept(visitor);
        if (parsedSimple instanceof Type) {
          cacheIfBounded(SIMPLE_CACHE, str, (Type) parsedSimple);
        }
        return parsedSimple;
      case PARAMETERIZED:
        ParameterizedType fastParam = PARAMETERIZED_FAST_PATH.get(str);
        if (fastParam != null) {
          return fastParam;
        }
        if (containsUserDefinedType(str)) {
          return parse(str).accept(visitor);
        }
        ParameterizedType cachedParam = PARAMETERIZED_CACHE.get(str);
        if (cachedParam != null) {
          return cachedParam;
        }
        TypeExpression parsedParam = parse(str).accept(visitor);
        if (parsedParam instanceof ParameterizedType) {
          cacheIfBounded(PARAMETERIZED_CACHE, str, (ParameterizedType) parsedParam);
        }
        return parsedParam;
      case EXPRESSION:
        return parseExpression(str, visitor.getUrn());
      default:
        return parse(str).accept(visitor);
    }
  }

  private static class TypeErrorListener extends BaseErrorListener {

    public static final TypeErrorListener INSTANCE = new TypeErrorListener();

    @Override
    public void syntaxError(
        final Recognizer<?, ?> recognizer,
        final Object offendingSymbol,
        final int line,
        final int charPositionInLine,
        final String msg,
        final RecognitionException e) {
      throw new ParseError(msg, e);
    }
  }

  /** Thrown when a type string cannot be parsed. */
  public static class ParseError extends RuntimeException {

    private static final long serialVersionUID = -6831467523614033666L;

    /**
     * Creates a parse error with the given message and cause.
     *
     * @param message the error message
     * @param cause the underlying recognition error
     */
    public ParseError(final String message, final Throwable cause) {
      super(message, cause);
    }
  }
}
