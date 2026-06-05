package io.substrait.plan;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Test cases for the Plan validation method that ensures ExecutionBehavior is properly configured.
 */
class PlanValidationTest {

  private final ProtoPlanConverter fromProtoConverter = new ProtoPlanConverter();

  private static io.substrait.proto.Version protoVersion(int major, int minor, int patch) {
    return io.substrait.proto.Version.newBuilder()
        .setMajorNumber(major)
        .setMinorNumber(minor)
        .setPatchNumber(patch)
        .build();
  }

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

  /**
   * Test case 4: Missing execution behavior on a pre-0.87.0 plan defaults to PER_PLAN.
   *
   * <p>ExecutionBehavior did not exist before Substrait 0.87.0, so converting a protobuf Plan that
   * explicitly declares an older version and omits execution behavior should default the variable
   * evaluation mode to PER_PLAN rather than failing validation.
   */
  @Test
  void testMissingExecutionBehaviorOnOldVersionDefaultsToPerPlan() {
    io.substrait.proto.Plan protoPlan =
        io.substrait.proto.Plan.newBuilder().setVersion(protoVersion(0, 86, 0)).build();

    Plan plan = assertDoesNotThrow(() -> fromProtoConverter.from(protoPlan));

    assertTrue(
        plan.getExecutionBehavior().isPresent(),
        "Pre-0.87.0 plan should have a defaulted ExecutionBehavior");
    assertEquals(
        Plan.ExecutionBehavior.VariableEvaluationMode.VARIABLE_EVALUATION_MODE_PER_PLAN,
        plan.getExecutionBehavior().get().getVariableEvaluationMode(),
        "Pre-0.87.0 plan should default to PER_PLAN");
  }

  /**
   * Test case 5: Missing execution behavior at exactly 0.87.0 still fails validation.
   *
   * <p>ExecutionBehavior is required from Substrait 0.87.0 onward, so a plan at that version with
   * no execution behavior must fail validation rather than being defaulted.
   */
  @Test
  void testMissingExecutionBehaviorAtCutoffVersionFails() {
    io.substrait.proto.Plan protoPlan =
        io.substrait.proto.Plan.newBuilder().setVersion(protoVersion(0, 87, 0)).build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> fromProtoConverter.from(protoPlan));
    assertEquals(
        "ExecutionBehavior is required but was not set",
        exception.getMessage(),
        "Plans at or after 0.87.0 require ExecutionBehavior");
  }

  /**
   * Test case 6: Missing execution behavior on a newer plan still fails validation.
   *
   * <p>Verifies the requirement also holds for versions beyond the 0.87.0 cutoff.
   */
  @Test
  void testMissingExecutionBehaviorOnNewerVersionFails() {
    io.substrait.proto.Plan protoPlan =
        io.substrait.proto.Plan.newBuilder().setVersion(protoVersion(1, 0, 0)).build();

    assertThrows(IllegalArgumentException.class, () -> fromProtoConverter.from(protoPlan));
  }

  /**
   * Test case 7: A version-less plan reads as the protobuf default 0.0.0.
   *
   * <p>A protobuf Plan that does not declare a version at all has the default version 0.0.0, which
   * is older than 0.87.0, so a missing execution behavior is defaulted to PER_PLAN.
   */
  @Test
  void testMissingExecutionBehaviorWithoutVersionDefaultsToPerPlan() {
    io.substrait.proto.Plan protoPlan = io.substrait.proto.Plan.newBuilder().build();

    Plan plan = assertDoesNotThrow(() -> fromProtoConverter.from(protoPlan));

    assertTrue(
        plan.getExecutionBehavior().isPresent(),
        "Version-less plan (0.0.0) should have a defaulted ExecutionBehavior");
    assertEquals(
        Plan.ExecutionBehavior.VariableEvaluationMode.VARIABLE_EVALUATION_MODE_PER_PLAN,
        plan.getExecutionBehavior().get().getVariableEvaluationMode(),
        "Version-less plan (0.0.0) should default to PER_PLAN");
  }

  /**
   * Test case 8: An explicit execution behavior on an old plan is not overridden.
   *
   * <p>When a pre-0.87.0 plan does provide an execution behavior, the provided value must be used
   * instead of the PER_PLAN default.
   */
  @Test
  void testExplicitExecutionBehaviorOnOldVersionIsPreserved() {
    io.substrait.proto.Plan protoPlan =
        io.substrait.proto.Plan.newBuilder()
            .setVersion(protoVersion(0, 86, 0))
            .setExecutionBehavior(
                io.substrait.proto.ExecutionBehavior.newBuilder()
                    .setVariableEvalMode(
                        io.substrait.proto.ExecutionBehavior.VariableEvaluationMode
                            .VARIABLE_EVALUATION_MODE_PER_RECORD)
                    .build())
            .build();

    Plan plan = fromProtoConverter.from(protoPlan);

    assertEquals(
        Plan.ExecutionBehavior.VariableEvaluationMode.VARIABLE_EVALUATION_MODE_PER_RECORD,
        plan.getExecutionBehavior().get().getVariableEvaluationMode(),
        "Explicit execution behavior should not be overridden by the default");
  }
}
