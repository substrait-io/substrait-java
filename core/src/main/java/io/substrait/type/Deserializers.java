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

public class Deserializers {

  public static final StdDeserializer<ParameterizedType> PARAMETERIZED_TYPE =
      new ParseDeserializer<>(ParameterizedType.class, ParseToPojo::parameterizedType);
  public static final StdDeserializer<Type> TYPE =
      new ParseDeserializer<>(Type.class, ParseToPojo::type);
  public static final StdDeserializer<TypeExpression> DERIVATION_EXPRESSION =
      new ParseDeserializer<>(TypeExpression.class, ParseToPojo::typeExpression);

  public static final SimpleModule MODULE =
      new SimpleModule()
          .addDeserializer(ParameterizedType.class, PARAMETERIZED_TYPE)
          .addDeserializer(TypeExpression.class, DERIVATION_EXPRESSION);

  public static class ParseDeserializer<T> extends StdDeserializer<T> {

    private final BiFunction<String, SubstraitTypeParser.StartContext, T> converter;

    public ParseDeserializer(
        Class<T> clazz, BiFunction<String, SubstraitTypeParser.StartContext, T> converter) {
      super(clazz);
      this.converter = converter;
    }

    @Override
    public T deserialize(final JsonParser p, final DeserializationContext ctxt)
        throws IOException, JsonProcessingException {
      String typeString = p.getValueAsString();
      try {
        String namespace =
            (String) ctxt.findInjectableValue(SimpleExtension.URN_LOCATOR_KEY, null, null);
        return TypeStringParser.parse(typeString, namespace, converter);
      } catch (Exception ex) {
        throw JsonMappingException.from(
            p, "Unable to parse string " + typeString.replace("\n", " \\n"), ex);
      }
    }
  }
}
