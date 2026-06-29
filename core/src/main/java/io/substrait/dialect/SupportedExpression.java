package io.substrait.dialect;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * A {@code supported_expressions} entry. Serializes as a bare enum string when it carries no
 * configuration, or as a configuration object otherwise.
 */
@JsonDeserialize(using = SupportedExpressionDeserializer.class)
@JsonSerialize(using = SupportedExpressionSerializer.class)
@Value.Immutable
public abstract class SupportedExpression {
  public abstract ExpressionKind expression();

  public abstract Optional<Map<String, Object>> metadata();

  /** Permissible failure options for {@code CAST}. */
  public abstract List<CastFailureOption> failureOptions();

  /** Subquery types for {@code SUBQUERY}. */
  public abstract List<SubqueryType> subqueryTypes();

  /** Nested types for {@code NESTED}. */
  public abstract List<NestedType> nestedTypes();

  /** Variable types for {@code EXECUTION_CONTEXT_VARIABLE}. */
  public abstract List<VariableType> variableTypes();

  /**
   * The schema's {@code execution_context_variable} entry forbids a {@code metadata} field, so
   * reject the combination rather than silently dropping it on serialization.
   */
  @Value.Check
  protected void checkMetadata() {
    if (expression() == ExpressionKind.EXECUTION_CONTEXT_VARIABLE && metadata().isPresent()) {
      throw new IllegalArgumentException(
          "EXECUTION_CONTEXT_VARIABLE expressions cannot carry metadata.");
    }
  }

  /** Whether this entry can be written as a bare enum string (no extra configuration). */
  public boolean isBare() {
    return !metadata().isPresent()
        && failureOptions().isEmpty()
        && subqueryTypes().isEmpty()
        && nestedTypes().isEmpty()
        && variableTypes().isEmpty();
  }

  public static SupportedExpression of(ExpressionKind expression) {
    return builder().expression(expression).build();
  }

  public static ImmutableSupportedExpression.Builder builder() {
    return ImmutableSupportedExpression.builder();
  }
}
