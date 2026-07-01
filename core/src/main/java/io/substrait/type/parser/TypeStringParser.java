package io.substrait.type.parser;

import io.substrait.function.ParameterizedType;
import io.substrait.function.TypeExpression;
import io.substrait.type.SubstraitTypeLexer;
import io.substrait.type.SubstraitTypeParser;
import io.substrait.type.Type;
import java.util.function.BiFunction;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

/** Parses Substrait type strings into their {@link TypeExpression} representations. */
public class TypeStringParser {

  private TypeStringParser() {}

  /**
   * Parses a string into a simple {@link Type}.
   *
   * @param str the type string to parse
   * @param urn the extension URN used to resolve user-defined types
   * @return the parsed type
   */
  public static Type parseSimple(String str, String urn) {
    return parse(str, urn, ParseToPojo::type);
  }

  /**
   * Parses a string into a {@link ParameterizedType}.
   *
   * @param str the type string to parse
   * @param urn the extension URN used to resolve user-defined types
   * @return the parsed parameterized type
   */
  public static ParameterizedType parseParameterized(String str, String urn) {
    return parse(str, urn, ParseToPojo::parameterizedType);
  }

  /**
   * Parses a string into a {@link TypeExpression}.
   *
   * @param str the type string to parse
   * @param urn the extension URN used to resolve user-defined types
   * @return the parsed type expression
   */
  public static TypeExpression parseExpression(String str, String urn) {
    return parse(str, urn, ParseToPojo::typeExpression);
  }

  private static SubstraitTypeParser.StartRuleContext parse(String str) {
    SubstraitTypeLexer lexer = new SubstraitTypeLexer(CharStreams.fromString(str));
    lexer.removeErrorListeners();
    lexer.addErrorListener(TypeErrorListener.INSTANCE);
    CommonTokenStream tokenStream = new CommonTokenStream(lexer);
    SubstraitTypeParser parser = new io.substrait.type.SubstraitTypeParser(tokenStream);
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
    return parse(str).accept(visitor);
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
