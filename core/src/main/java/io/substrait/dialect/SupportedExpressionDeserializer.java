package io.substrait.dialect;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Map;

/**
 * Deserializes a {@code supported_expressions} entry, which is either a bare enum string (e.g.
 * {@code LITERAL}) or a configuration object (e.g. {@code {expression: CAST, failure_options:
 * [RETURN_NULL]}}).
 */
class SupportedExpressionDeserializer extends JsonDeserializer<SupportedExpression> {

  @Override
  public SupportedExpression deserialize(JsonParser p, DeserializationContext ctxt)
      throws IOException {
    JsonNode node = p.getCodec().readTree(p);
    if (node.isTextual()) {
      return SupportedExpression.of(ExpressionKind.valueOf(node.asText()));
    }

    ExpressionKind kind = ExpressionKind.valueOf(node.get("expression").asText());
    ImmutableSupportedExpression.Builder builder = SupportedExpression.builder().expression(kind);

    Map<String, Object> metadata = DialectJsonSupport.readMetadata(node);
    if (metadata != null) {
      builder.metadata(metadata);
    }
    builder.addAllFailureOptions(
        DialectJsonSupport.readEnums(node.get("failure_options"), CastFailureOption.class));
    builder.addAllSubqueryTypes(
        DialectJsonSupport.readEnums(node.get("subquery_types"), SubqueryType.class));
    builder.addAllNestedTypes(
        DialectJsonSupport.readEnums(node.get("nested_types"), NestedType.class));
    builder.addAllVariableTypes(
        DialectJsonSupport.readEnums(node.get("variable_types"), VariableType.class));

    return builder.build();
  }
}
