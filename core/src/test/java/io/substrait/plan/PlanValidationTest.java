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
   * <p>This test verifies that attempting to create a Plan without setting the ExecutionBehavior
   * field throws an IllegalArgumentException with an appropriate error message indicating that
   * ExecutionBehavior is required.
   */
  @Test
  void testMissingExecutionBehavior() {
    // Attempt to create a Plan without ExecutionBehavior
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              ImmutablePlan.builder().build();
            },
            "Plan creation should fail when ExecutionBehavior is not set");

    // Verify the error message
    assertEquals(
        "ExecutionBehavior is required but was not set",
        exception.getMessage(),
        "Error message should indicate ExecutionBehavior is required");
  }

  /**
   * Test case 3: Execution behavior with unspecified variable evaluation mode.
   *
   * <p>This test verifies that attempting to create a Plan with an ExecutionBehavior that has its
   * VariableEvaluationMode set to VARIABLE_EVALUATION_MODE_UNSPECIFIED throws an
   * IllegalArgumentException with an appropriate error message.
   */
  @Test
  void testUnspecifiedVariableEvaluationMode() {
    // Create an ExecutionBehavior with VARIABLE_EVALUATION_MODE_UNSPECIFIED
    Plan.ExecutionBehavior executionBehavior =
        ImmutableExecutionBehavior.builder()
            .variableEvaluationMode(
                Plan.ExecutionBehavior.VariableEvaluationMode.VARIABLE_EVALUATION_MODE_UNSPECIFIED)
            .build();

    // Attempt to create a Plan with the invalid ExecutionBehavior
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              ImmutablePlan.builder().executionBehavior(executionBehavior).build();
            },
            "Plan creation should fail when VariableEvaluationMode is UNSPECIFIED");

    // Verify the error message contains the expected information
    String expectedMessage =
        "ExecutionBehavior requires a specified VariableEvaluationMode, but got: "
            + "VARIABLE_EVALUATION_MODE_UNSPECIFIED";
    assertEquals(
        expectedMessage,
        exception.getMessage(),
        "Error message should indicate VariableEvaluationMode cannot be UNSPECIFIED");
  }
}
