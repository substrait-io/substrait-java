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

public class TypeStringParser {

  private TypeStringParser() {}

  public static Type parseSimple(final String str, final String urn) {
    return parse(str, urn, ParseToPojo::type);
  }

  public static ParameterizedType parseParameterized(final String str, final String urn) {
    return parse(str, urn, ParseToPojo::parameterizedType);
  }

  public static TypeExpression parseExpression(final String str, final String urn) {
    return parse(str, urn, ParseToPojo::typeExpression);
  }

  private static SubstraitTypeParser.StartContext parse(final String str) {
    final SubstraitTypeLexer lexer = new SubstraitTypeLexer(CharStreams.fromString(str));
    lexer.removeErrorListeners();
    lexer.addErrorListener(TypeErrorListener.INSTANCE);
    final CommonTokenStream tokenStream = new CommonTokenStream(lexer);
    final SubstraitTypeParser parser = new io.substrait.type.SubstraitTypeParser(tokenStream);
    parser.removeErrorListeners();
    parser.addErrorListener(TypeErrorListener.INSTANCE);
    return parser.start();
  }

  public static <T> T parse(
      final String str,
      final String urn,
      final BiFunction<String, SubstraitTypeParser.StartContext, T> func) {
    return func.apply(urn, parse(str));
  }

  public static TypeExpression parse(final String str, final ParseToPojo.Visitor visitor) {
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

  public static class ParseError extends RuntimeException {

    private static final long serialVersionUID = -6831467523614033666L;

    public ParseError(final String message, final Throwable cause) {
      super(message, cause);
    }
  }
}
