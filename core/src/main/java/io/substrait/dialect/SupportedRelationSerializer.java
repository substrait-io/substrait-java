package io.substrait.dialect;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;

/**
 * Serializes a {@code supported_relations} entry as a bare enum string when it carries no
 * configuration, or as a configuration object otherwise.
 */
class SupportedRelationSerializer extends JsonSerializer<SupportedRelation> {

  @Override
  public void serialize(SupportedRelation value, JsonGenerator gen, SerializerProvider provider)
      throws IOException {
    if (value.isBare()) {
      gen.writeString(value.relation().name());
      return;
    }

    gen.writeStartObject();
    gen.writeStringField("relation", value.relation().name());
    if (!value.joinTypes().isEmpty()) {
      DialectJsonSupport.writeEnumArray(gen, "join_types", value.joinTypes());
    }
    if (!value.readTypes().isEmpty()) {
      DialectJsonSupport.writeEnumArray(gen, "read_types", value.readTypes());
    }
    if (!value.operations().isEmpty()) {
      DialectJsonSupport.writeEnumArray(gen, "operations", value.operations());
    }
    // WRITE and DDL share the `write_types` field; SupportedRelation forbids populating both.
    if (!value.writeTypes().isEmpty()) {
      DialectJsonSupport.writeEnumArray(gen, "write_types", value.writeTypes());
    }
    if (!value.ddlWriteTypes().isEmpty()) {
      DialectJsonSupport.writeEnumArray(gen, "write_types", value.ddlWriteTypes());
    }
    if (!value.kinds().isEmpty()) {
      DialectJsonSupport.writeEnumArray(gen, "kinds", value.kinds());
    }
    if (!value.fieldTypes().isEmpty()) {
      DialectJsonSupport.writeEnumArray(gen, "field_types", value.fieldTypes());
    }
    if (!value.messageTypes().isEmpty()) {
      DialectJsonSupport.writeStringArray(gen, "message_types", value.messageTypes());
    }
    if (value.metadata().isPresent()) {
      DialectJsonSupport.writeMetadata(gen, provider, value.metadata().get());
    }
    gen.writeEndObject();
  }
}
