package io.substrait.type;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.substrait.antlr.SubstraitTypeParser;
import io.substrait.extension.SimpleExtension;
import io.substrait.function.ParameterizedType;
import io.substrait.function.TypeExpression;
import io.substrait.type.parser.TypeStringParser;
import java.io.IOException;
import java.util.function.BiFunction;

/**
 * Provides Jackson deserializers for Substrait type-related classes. These deserializers parse JSON
 * representations of types into their corresponding Java objects.
 */
public class Deserializers {

  /** Deserializer for {@link ParameterizedType} from JSON representation. */
  public static final StdDeserializer<ParameterizedType> PARAMETERIZED_TYPE =
      ParseDeserializer.of(ParameterizedType.class, TypeStringParser::parseParameterized);

  /** Deserializer for {@link Type} from JSON representation. */
  public static final StdDeserializer<Type> TYPE =
      ParseDeserializer.of(Type.class, TypeStringParser::parseSimple);

  /** Deserializer for {@link TypeExpression} from JSON representation. */
  public static final StdDeserializer<TypeExpression> DERIVATION_EXPRESSION =
      ParseDeserializer.of(TypeExpression.class, TypeStringParser::parseExpression);

  /** Jackson module containing all type deserializers. */
  public static final SimpleModule MODULE =
      new SimpleModule()
          .addDeserializer(ParameterizedType.class, PARAMETERIZED_TYPE)
          .addDeserializer(TypeExpression.class, DERIVATION_EXPRESSION);

  /**
   * Generic deserializer that parses JSON representations of types using a provided converter
   * function.
   *
   * @param <T> the type to deserialize
   */
  public static class ParseDeserializer<T> extends StdDeserializer<T> {

    private static final long serialVersionUID = 2105956703553161270L;

    /** Parses a type string and URN into the target type {@code T}. */
    private final StringParser<T> parser;

    /**
     * Constructs a ParseDeserializer with the specified class and converter function.
     *
     * @param clazz the class to deserialize
     * @param converter the function to convert parsed JSON to the target type
     */
    public ParseDeserializer(
        Class<T> clazz, BiFunction<String, SubstraitTypeParser.StartRuleContext, T> converter) {
      super(clazz);
      this.parser = (typeString, urn) -> TypeStringParser.parse(typeString, urn, converter);
    }

    private ParseDeserializer(Class<T> clazz, StringParser<T> parser) {
      super(clazz);
      this.parser = parser;
    }

    private static <T> ParseDeserializer<T> of(Class<T> clazz, StringParser<T> parser) {
      return new ParseDeserializer<>(clazz, parser);
    }

    @Override
    public T deserialize(final JsonParser p, final DeserializationContext ctxt)
        throws IOException, JsonProcessingException {
      String typeString = p.getValueAsString();
      try {
        String urn = (String) ctxt.findInjectableValue(SimpleExtension.URN_LOCATOR_KEY, null, null);
        return parser.parse(typeString, urn);
      } catch (Exception ex) {
        throw JsonMappingException.from(
            p, "Unable to parse string " + typeString.replace("\n", " \\n"), ex);
      }
    }

    private interface StringParser<T> {
      T parse(String typeString, String urn);
    }
  }
}
