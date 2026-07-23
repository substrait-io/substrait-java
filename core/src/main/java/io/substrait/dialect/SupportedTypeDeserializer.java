package io.substrait.dialect;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Map;

/**
 * Deserializes a {@code supported_types} entry, which is either a bare enum string (e.g. {@code
 * BOOL}) or a configuration object (e.g. {@code {type: PRECISION_TIMESTAMP, max_precision: 9}}).
 */
class SupportedTypeDeserializer extends JsonDeserializer<SupportedType> {

  @Override
  public SupportedType deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    JsonNode node = p.getCodec().readTree(p);
    if (node.isTextual()) {
      return SupportedType.of(TypeKind.valueOf(node.asText()));
    }

    ImmutableSupportedType.Builder builder =
        SupportedType.builder().type(TypeKind.valueOf(node.get("type").asText()));

    Map<String, Object> metadata = DialectJsonSupport.readMetadata(node);
    if (metadata != null) {
      builder.metadata(metadata);
    }
    if (node.hasNonNull("system_metadata")) {
      builder.systemMetadata(
          DialectJsonSupport.MAPPER.convertValue(
              node.get("system_metadata"), SystemTypeMetadata.class));
    }
    if (node.hasNonNull("max_precision")) {
      builder.maxPrecision(node.get("max_precision").asInt());
    }
    if (node.hasNonNull("max_scale")) {
      builder.maxScale(node.get("max_scale").asInt());
    }
    if (node.hasNonNull("max_length")) {
      builder.maxLength(node.get("max_length").asInt());
    }
    if (node.hasNonNull("source")) {
      builder.source(node.get("source").asText());
    }
    if (node.hasNonNull("name")) {
      builder.name(node.get("name").asText());
    }

    return builder.build();
  }
}
