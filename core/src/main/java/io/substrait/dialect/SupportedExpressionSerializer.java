package io.substrait.dialect;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.substrait.dialect.Dialect.ExpressionKind;
import io.substrait.dialect.Dialect.SupportedExpression;
import java.io.IOException;

/**
 * Serializes a {@code supported_expressions} entry as a bare enum string when it carries no
 * configuration, or as a configuration object otherwise.
 */
public class SupportedExpressionSerializer extends JsonSerializer<SupportedExpression> {

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
    // The schema forbids `metadata` on EXECUTION_CONTEXT_VARIABLE, so only emit it for others.
    if (value.metadata().isPresent()
        && value.expression() != ExpressionKind.EXECUTION_CONTEXT_VARIABLE) {
      DialectJsonSupport.writeMetadata(gen, provider, value.metadata().get());
    }
    gen.writeEndObject();
  }
}
