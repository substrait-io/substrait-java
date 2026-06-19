package io.substrait.plan;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Test cases for the Plan validation method that ensures ExecutionBehavior is properly configured.
 */
class PlanValidationTest {

  /**
   * Valid execution behavior with a specified variable evaluation mode.
   *
   * <p>This test verifies that a Plan with a properly configured ExecutionBehavior (with
   * VariableEvaluationMode set to a valid value other than UNSPECIFIED) is successfully created
   * without throwing any exceptions.
   */
  @ParameterizedTest
  @EnumSource(
      value = Plan.ExecutionBehavior.VariableEvaluationMode.class,
      names = {"PER_PLAN", "PER_RECORD"})
  void testValidExecutionBehavior(Plan.ExecutionBehavior.VariableEvaluationMode mode) {
    // Create a valid ExecutionBehavior
    Plan.ExecutionBehavior executionBehavior =
        ImmutableExecutionBehavior.builder().variableEvaluationMode(mode).build();

    // Create a Plan with the valid ExecutionBehavior
    assertDoesNotThrow(
        () -> {
          ImmutablePlan.builder().executionBehavior(executionBehavior).build();
        },
        "Plan creation should succeed with valid ExecutionBehavior");
  }

  /**
   * Missing execution behavior.
   *
   * <p>This test verifies that attempting to create a Plan without setting the ExecutionBehavior
   * field throws an IllegalStateException (from Immutables) with an appropriate error message
   * indicating that ExecutionBehavior is required.
   */
  @Test
  void testMissingExecutionBehavior() {
    // Attempt to create a Plan without ExecutionBehavior
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> {
              ImmutablePlan.builder().build();
            },
            "Plan creation should fail when ExecutionBehavior is not set");

    // Verify the error message mentions the required field
    assertTrue(
        exception.getMessage().contains("executionBehavior"),
        "Error message should mention executionBehavior field");
  }

  /**
   * Execution behavior with unspecified variable evaluation mode.
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
            .variableEvaluationMode(Plan.ExecutionBehavior.VariableEvaluationMode.UNSPECIFIED)
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
        "ExecutionBehavior requires a specified VariableEvaluationMode, but got: " + "UNSPECIFIED";
    assertEquals(
        expectedMessage,
        exception.getMessage(),
        "Error message should indicate VariableEvaluationMode cannot be UNSPECIFIED");
  }
}
