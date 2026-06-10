package io.substrait.dialect;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.substrait.dialect.Dialect.SupportedType;
import java.io.IOException;

/**
 * Serializes a {@code supported_types} entry as a bare enum string when it carries no
 * configuration, or as a configuration object otherwise.
 */
public class SupportedTypeSerializer extends JsonSerializer<SupportedType> {

  @Override
  public void serialize(SupportedType value, JsonGenerator gen, SerializerProvider provider)
      throws IOException {
    if (value.isBare()) {
      gen.writeString(value.type().name());
      return;
    }

    gen.writeStartObject();
    gen.writeStringField("type", value.type().name());
    if (value.source().isPresent()) {
      gen.writeStringField("source", value.source().get());
    }
    if (value.name().isPresent()) {
      gen.writeStringField("name", value.name().get());
    }
    if (value.maxPrecision().isPresent()) {
      gen.writeNumberField("max_precision", value.maxPrecision().get());
    }
    if (value.systemMetadata().isPresent()) {
      provider.defaultSerializeField("system_metadata", value.systemMetadata().get(), gen);
    }
    if (value.metadata().isPresent()) {
      DialectJsonSupport.writeMetadata(gen, provider, value.metadata().get());
    }
    gen.writeEndObject();
  }
}
