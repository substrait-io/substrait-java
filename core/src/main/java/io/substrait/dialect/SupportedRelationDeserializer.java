package io.substrait.dialect;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Map;

/**
 * Deserializes a {@code supported_relations} entry, which is either a bare enum string (e.g. {@code
 * FILTER}) or a configuration object (e.g. {@code {relation: JOIN, join_types: [INNER]}}).
 */
class SupportedRelationDeserializer extends JsonDeserializer<SupportedRelation> {

  @Override
  public SupportedRelation deserialize(JsonParser p, DeserializationContext ctxt)
      throws IOException {
    JsonNode node = p.getCodec().readTree(p);
    if (node.isTextual()) {
      return SupportedRelation.of(RelationKind.valueOf(node.asText()));
    }

    RelationKind kind = RelationKind.valueOf(node.get("relation").asText());
    ImmutableSupportedRelation.Builder builder = SupportedRelation.builder().relation(kind);

    Map<String, Object> metadata = DialectJsonSupport.readMetadata(node);
    if (metadata != null) {
      builder.metadata(metadata);
    }
    builder.addAllJoinTypes(DialectJsonSupport.readEnums(node.get("join_types"), JoinType.class));
    builder.addAllReadTypes(DialectJsonSupport.readEnums(node.get("read_types"), ReadType.class));
    builder.addAllOperations(
        DialectJsonSupport.readEnums(node.get("operations"), SetOperation.class));
    builder.addAllKinds(DialectJsonSupport.readEnums(node.get("kinds"), ExchangeKind.class));
    builder.addAllFieldTypes(
        DialectJsonSupport.readEnums(node.get("field_types"), ExpandFieldType.class));
    builder.addAllMessageTypes(DialectJsonSupport.readStrings(node.get("message_types")));

    // `write_types` is shared between WRITE and DDL but uses a different enum for each.
    if (kind == RelationKind.DDL) {
      builder.addAllDdlWriteTypes(
          DialectJsonSupport.readEnums(node.get("write_types"), DdlWriteType.class));
    } else {
      builder.addAllWriteTypes(
          DialectJsonSupport.readEnums(node.get("write_types"), WriteType.class));
    }

    return builder.build();
  }
}
