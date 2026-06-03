package io.substrait.plan;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Test cases for the Plan validation method that ensures ExecutionBehavior is properly configured.
 */
class PlanValidationTest {

  /**
   * Test case 1: Valid execution behavior with proper variable evaluation mode.
   *
   * <p>This test verifies that a Plan with a properly configured ExecutionBehavior (with
   * VariableEvaluationMode set to a valid value other than UNSPECIFIED) is successfully created
   * without throwing any exceptions.
   */
  @Test
  void testValidExecutionBehavior_PerPlan() {
    // Create a valid ExecutionBehavior with VARIABLE_EVALUATION_MODE_PER_PLAN
    Plan.ExecutionBehavior executionBehavior =
        ImmutableExecutionBehavior.builder()
            .variableEvaluationMode(
                Plan.ExecutionBehavior.VariableEvaluationMode.VARIABLE_EVALUATION_MODE_PER_PLAN)
            .build();

    // Create a Plan with the valid ExecutionBehavior
    assertDoesNotThrow(
        () -> {
          ImmutablePlan.builder().executionBehavior(executionBehavior).build();
        },
        "Plan creation should succeed with valid ExecutionBehavior");
  }

  /**
   * Test case 1b: Valid execution behavior with VARIABLE_EVALUATION_MODE_PER_RECORD.
   *
   * <p>This test verifies that a Plan with ExecutionBehavior set to
   * VARIABLE_EVALUATION_MODE_PER_RECORD is also valid.
   */
  @Test
  void testValidExecutionBehavior_PerRecord() {
    // Create a valid ExecutionBehavior with VARIABLE_EVALUATION_MODE_PER_RECORD
    Plan.ExecutionBehavior executionBehavior =
        ImmutableExecutionBehavior.builder()
            .variableEvaluationMode(
                Plan.ExecutionBehavior.VariableEvaluationMode.VARIABLE_EVALUATION_MODE_PER_RECORD)
            .build();

    // Create a Plan with the valid ExecutionBehavior
    assertDoesNotThrow(
        () -> {
          ImmutablePlan.builder().executionBehavior(executionBehavior).build();
        },
        "Plan creation should succeed with valid ExecutionBehavior");
  }

  /**
   * Test case 2: Missing execution behavior (empty Optional).
   *
   * <p>This test verifies that a Plan can be built without ExecutionBehavior, but serialization via
   * [`PlanProtoConverter.toProto()`](core/src/main/java/io/substrait/plan/PlanProtoConverter.java:86)
   * fails with an IllegalArgumentException indicating that ExecutionBehavior is required.
   */
  @Test
  void testMissingExecutionBehavior() {
    Plan plan = ImmutablePlan.builder().build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> new PlanProtoConverter().toProto(plan),
            "Plan serialization should fail when ExecutionBehavior is not set");

    assertEquals(
        "ExecutionBehavior is required since Substrait v0.87.0 but was not set",
        exception.getMessage(),
        "Error message should indicate ExecutionBehavior is required");
  }

  /**
   * Test case 3: Execution behavior with unspecified variable evaluation mode.
   *
   * <p>This test verifies that a Plan can be built with an ExecutionBehavior whose
   * VariableEvaluationMode is VARIABLE_EVALUATION_MODE_UNSPECIFIED, but serialization via
   * [`PlanProtoConverter.toProto()`](core/src/main/java/io/substrait/plan/PlanProtoConverter.java:86)
   * fails with an IllegalArgumentException.
   */
  @Test
  void testUnspecifiedVariableEvaluationMode() {
    Plan.ExecutionBehavior executionBehavior =
        ImmutableExecutionBehavior.builder()
            .variableEvaluationMode(
                Plan.ExecutionBehavior.VariableEvaluationMode.VARIABLE_EVALUATION_MODE_UNSPECIFIED)
            .build();

    Plan plan = ImmutablePlan.builder().executionBehavior(executionBehavior).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> new PlanProtoConverter().toProto(plan),
            "Plan serialization should fail when VariableEvaluationMode is UNSPECIFIED");

    String expectedMessage =
        "ExecutionBehavior requires a specified VariableEvaluationMode, but got: "
            + "VARIABLE_EVALUATION_MODE_UNSPECIFIED";
    assertEquals(
        expectedMessage,
        exception.getMessage(),
        "Error message should indicate VariableEvaluationMode cannot be UNSPECIFIED");
  }
}
