package io.substrait.dialect;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.substrait.dialect.Dialect.SupportedType;
import io.substrait.dialect.Dialect.SystemTypeMetadata;
import io.substrait.dialect.Dialect.TypeKind;
import java.io.IOException;
import java.util.Map;

/**
 * Deserializes a {@code supported_types} entry, which is either a bare enum string (e.g. {@code
 * BOOL}) or a configuration object (e.g. {@code {type: PRECISION_TIMESTAMP, max_precision: 9}}).
 */
public class SupportedTypeDeserializer extends JsonDeserializer<SupportedType> {

  @Override
  public SupportedType deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    JsonNode node = p.getCodec().readTree(p);
    if (node.isTextual()) {
      return SupportedType.of(TypeKind.valueOf(node.asText()));
    }

    ObjectMapper mapper = (ObjectMapper) p.getCodec();
    ImmutableDialect.SupportedType.Builder builder =
        SupportedType.builder().type(TypeKind.valueOf(node.get("type").asText()));

    Map<String, Object> metadata = DialectJsonSupport.readMetadata(p, node);
    if (metadata != null) {
      builder.metadata(metadata);
    }
    if (node.hasNonNull("system_metadata")) {
      builder.systemMetadata(
          mapper.convertValue(node.get("system_metadata"), SystemTypeMetadata.class));
    }
    if (node.hasNonNull("max_precision")) {
      builder.maxPrecision(node.get("max_precision").asInt());
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
