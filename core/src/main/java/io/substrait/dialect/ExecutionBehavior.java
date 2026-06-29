package io.substrait.dialect;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import org.immutables.value.Value;

/** Execution-behavior configuration for a dialect. */
@JsonDeserialize(as = ImmutableExecutionBehavior.class)
@JsonSerialize(as = ImmutableExecutionBehavior.class)
@Value.Immutable
public abstract class ExecutionBehavior {
  @JsonProperty("supported_variable_evaluation_mode")
  public abstract List<VariableEvaluationMode> supportedVariableEvaluationMode();

  public static ImmutableExecutionBehavior.Builder builder() {
    return ImmutableExecutionBehavior.builder();
  }
}
