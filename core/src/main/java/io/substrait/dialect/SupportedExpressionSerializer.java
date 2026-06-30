package io.substrait.dialect;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;

/**
 * Serializes a {@code supported_expressions} entry as a bare enum string when it carries no
 * configuration, or as a configuration object otherwise.
 */
class SupportedExpressionSerializer extends JsonSerializer<SupportedExpression> {

  @Override
  public void serialize(SupportedExpression value, JsonGenerator gen, SerializerProvider provider)
      throws IOException {
    if (value.isBare()) {
      gen.writeString(value.expression().name());
      return;
    }

    gen.writeStartObject();
    gen.writeStringField("expression", value.expression().name());
    if (!value.failureOptions().isEmpty()) {
      DialectJsonSupport.writeEnumArray(gen, "failure_options", value.failureOptions());
    }
    if (!value.subqueryTypes().isEmpty()) {
      DialectJsonSupport.writeEnumArray(gen, "subquery_types", value.subqueryTypes());
    }
    if (!value.nestedTypes().isEmpty()) {
      DialectJsonSupport.writeEnumArray(gen, "nested_types", value.nestedTypes());
    }
    if (!value.variableTypes().isEmpty()) {
      DialectJsonSupport.writeEnumArray(gen, "variable_types", value.variableTypes());
    }
    // EXECUTION_CONTEXT_VARIABLE cannot carry metadata (enforced by SupportedExpression).
    if (value.metadata().isPresent()) {
      DialectJsonSupport.writeMetadata(gen, provider, value.metadata().get());
    }
    gen.writeEndObject();
  }
}
