package io.substrait.type;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.substrait.extension.SimpleExtension;
import io.substrait.function.ParameterizedType;
import io.substrait.function.TypeExpression;
import io.substrait.type.parser.ParseToPojo;
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
      new ParseDeserializer<>(ParameterizedType.class, ParseToPojo::parameterizedType);

  /** Deserializer for {@link Type} from JSON representation. */
  public static final StdDeserializer<Type> TYPE =
      new ParseDeserializer<>(Type.class, ParseToPojo::type);

  /** Deserializer for {@link TypeExpression} from JSON representation. */
  public static final StdDeserializer<TypeExpression> DERIVATION_EXPRESSION =
      new ParseDeserializer<>(TypeExpression.class, ParseToPojo::typeExpression);

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

    private final BiFunction<String, SubstraitTypeParser.StartRuleContext, T> converter;

    /**
     * Constructs a ParseDeserializer with the specified class and converter function.
     *
     * @param clazz the class to deserialize
     * @param converter the function to convert parsed JSON to the target type
     */
    public ParseDeserializer(
        Class<T> clazz, BiFunction<String, SubstraitTypeParser.StartRuleContext, T> converter) {
      super(clazz);
      this.converter = converter;
    }

    @Override
    public T deserialize(final JsonParser p, final DeserializationContext ctxt)
        throws IOException, JsonProcessingException {
      String typeString = p.getValueAsString();
      try {
        String urn = (String) ctxt.findInjectableValue(SimpleExtension.URN_LOCATOR_KEY, null, null);
        return TypeStringParser.parse(typeString, urn, converter);
      } catch (Exception ex) {
        throw JsonMappingException.from(
            p, "Unable to parse string " + typeString.replace("\n", " \\n"), ex);
      }
    }
  }
}
