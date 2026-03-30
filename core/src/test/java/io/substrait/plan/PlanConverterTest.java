package io.substrait.plan;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.AdvancedExtension;
import io.substrait.extension.SimpleExtension;
import io.substrait.plan.Plan.Root;
import io.substrait.relation.ImmutableVirtualTableScan;
import io.substrait.relation.VirtualTableScan;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.utils.StringHolder;
import io.substrait.utils.StringHolderHandlingExtensionProtoConverter;
import io.substrait.utils.StringHolderHandlingProtoExtensionConverter;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class PlanConverterTest {
  private final PlanProtoConverter toProtoConverter = new PlanProtoConverter();
  private final ProtoPlanConverter fromProtoConverter = new ProtoPlanConverter();

  private static Plan.ExecutionBehavior defaultExecutionBehavior() {
    return ImmutableExecutionBehavior.builder()
        .variableEvaluationMode(
            Plan.ExecutionBehavior.VariableEvaluationMode.VARIABLE_EVALUATION_MODE_PER_PLAN)
        .build();
  }

  @Test
  void emptyAdvancedExtensionTest() {
    final Plan plan =
        Plan.builder()
            .executionBehavior(defaultExecutionBehavior())
            .advancedExtension(AdvancedExtension.builder().build())
            .build();
    final io.substrait.proto.Plan protoPlan = toProtoConverter.toProto(plan);

    final Plan plan2 = fromProtoConverter.from(protoPlan);

    assertEquals(plan, plan2);
  }

  @Test
  void enhancementOnlyAdvancedExtensionWithoutExtensionProtoConverter() {
    final StringHolder enhanced = new StringHolder("ENHANCED");

    final Plan plan =
        Plan.builder()
            .executionBehavior(defaultExecutionBehavior())
            .advancedExtension(AdvancedExtension.builder().enhancement(enhanced).build())
            .build();

    assertThrows(
        UnsupportedOperationException.class,
        () -> toProtoConverter.toProto(plan),
        "missing serialization logic for AdvancedExtension.Enhancement");
  }

  @Test
  void enhancementOnlyAdvancedExtensionWithExtensionProtoConverter() {
    final StringHolder enhanced = new StringHolder("ENHANCED");

    final Plan plan =
        Plan.builder()
            .executionBehavior(defaultExecutionBehavior())
            .advancedExtension(AdvancedExtension.builder().enhancement(enhanced).build())
            .build();
    final PlanProtoConverter toProtoConverter =
        new PlanProtoConverter(new StringHolderHandlingExtensionProtoConverter());
    final io.substrait.proto.Plan protoPlan = toProtoConverter.toProto(plan);

    assertThrows(
        UnsupportedOperationException.class,
        () -> fromProtoConverter.from(protoPlan),
        "missing deserialization logic for AdvancedExtension.Enhancement");
  }

  @Test
  void enhancementOnlyAdvancedExtensionWithExtensionProtoConverterAndProtoExtensionConverter() {
    final StringHolder enhanced = new StringHolder("ENHANCED");

    final Plan plan =
        Plan.builder()
            .executionBehavior(defaultExecutionBehavior())
            .advancedExtension(AdvancedExtension.builder().enhancement(enhanced).build())
            .build();
    final PlanProtoConverter toProtoConverter =
        new PlanProtoConverter(new StringHolderHandlingExtensionProtoConverter());
    final io.substrait.proto.Plan protoPlan = toProtoConverter.toProto(plan);

    final ProtoPlanConverter fromProtoConverter =
        new ProtoPlanConverter(new StringHolderHandlingProtoExtensionConverter());
    final Plan plan2 = fromProtoConverter.from(protoPlan);

    assertEquals(plan, plan2);
  }

  @Test
  void optimizationOnlyAdvancedExtensionWithoutExtensionProtoConverter() {
    final StringHolder optimized = new StringHolder("OPTIMIZED");

    final Plan plan =
        Plan.builder()
            .executionBehavior(defaultExecutionBehavior())
            .advancedExtension(AdvancedExtension.builder().addOptimizations(optimized).build())
            .build();

    assertThrows(
        UnsupportedOperationException.class,
        () -> toProtoConverter.toProto(plan),
        "missing sserialization logic for AdvancedExtension.Optimization");
  }

  @Test
  void optimizationOnlyAdvancedExtensionWithExtensionProtoConverter() {
    final StringHolder optimized = new StringHolder("OPTIMIZED");

    final Plan plan =
        Plan.builder()
            .executionBehavior(defaultExecutionBehavior())
            .advancedExtension(AdvancedExtension.builder().addOptimizations(optimized).build())
            .build();
    final PlanProtoConverter toProtoConverter =
        new PlanProtoConverter(new StringHolderHandlingExtensionProtoConverter());
    final io.substrait.proto.Plan protoPlan = toProtoConverter.toProto(plan);

    assertThrows(
        UnsupportedOperationException.class,
        () -> fromProtoConverter.from(protoPlan),
        "missing deserialization logic for AdvancedExtension.Optimization");
  }

  @Test
  void optimizationOnlyAdvancedExtensionWithExtensionProtoConverterAndProtoExtensionConverter() {
    final StringHolder optimized = new StringHolder("OPTIMIZED");

    final Plan plan =
        Plan.builder()
            .executionBehavior(defaultExecutionBehavior())
            .advancedExtension(AdvancedExtension.builder().addOptimizations(optimized).build())
            .build();
    final PlanProtoConverter toProtoConverter =
        new PlanProtoConverter(new StringHolderHandlingExtensionProtoConverter());
    final io.substrait.proto.Plan protoPlan = toProtoConverter.toProto(plan);

    final ProtoPlanConverter fromProtoConverter =
        new ProtoPlanConverter(new StringHolderHandlingProtoExtensionConverter());
    final Plan plan2 = fromProtoConverter.from(protoPlan);

    assertEquals(plan, plan2);
  }

  @Test
  void advancedExtensionWithEnhancementAndOptimization() {
    final StringHolder enhanced = new StringHolder("ENHANCED");
    final StringHolder optimized = new StringHolder("OPTIMIZED");

    final Plan plan =
        Plan.builder()
            .executionBehavior(defaultExecutionBehavior())
            .advancedExtension(
                AdvancedExtension.builder()
                    .enhancement(enhanced)
                    .addOptimizations(optimized)
                    .build())
            .build();
    final PlanProtoConverter toProtoConverter =
        new PlanProtoConverter(new StringHolderHandlingExtensionProtoConverter());
    final io.substrait.proto.Plan protoPlan = toProtoConverter.toProto(plan);

    final ProtoPlanConverter fromProtoConverter =
        new ProtoPlanConverter(new StringHolderHandlingProtoExtensionConverter());
    final Plan plan2 = fromProtoConverter.from(protoPlan);

    assertEquals(plan, plan2);
  }

  @Test
  void planIncludingRelationWithAdvancedExtension() {
    final StringHolder enhanced = new StringHolder("ENHANCED");
    final StringHolder optimized = new StringHolder("OPTIMIZED");

    final Plan plan =
        Plan.builder()
            .executionBehavior(defaultExecutionBehavior())
            .addRoots(
                Root.builder()
                    .input(
                        VirtualTableScan.builder()
                            .initialSchema(
                                NamedStruct.builder().struct(TypeCreator.REQUIRED.struct()).build())
                            .extension(
                                AdvancedExtension.builder()
                                    .enhancement(enhanced)
                                    .addOptimizations(optimized)
                                    .build())
                            .build())
                    .build())
            .build();
    final PlanProtoConverter toProtoConverter =
        new PlanProtoConverter(new StringHolderHandlingExtensionProtoConverter());
    final io.substrait.proto.Plan protoPlan = toProtoConverter.toProto(plan);

    final ProtoPlanConverter fromProtoConverter =
        new ProtoPlanConverter(new StringHolderHandlingProtoExtensionConverter());
    final Plan plan2 = fromProtoConverter.from(protoPlan);

    assertEquals(plan, plan2);
  }

  /**
   * Verifies that nested UserDefined types with type parameters share the same ExtensionCollector
   * and don't create duplicate type references. Tests that a plan containing both a standalone
   * UserDefined literal (point) and a parameterized UserDefined literal (vector<point>) correctly
   * registers both types in the extension collection without duplication.
   */
  @Test
  void nestedUserDefinedTypesShareExtensionCollector() {
    // Define custom types: point and vector<T>
    String urn = "extension:test:nested_types";
    String yaml =
        "---\n"
            + "urn: "
            + urn
            + "\n"
            + "types:\n"
            + "  - name: point\n"
            + "    structure:\n"
            + "      x: i32\n"
            + "      y: i32\n"
            + "  - name: vector\n"
            + "    parameters:\n"
            + "      - name: T\n"
            + "        type: dataType\n"
            + "    structure:\n"
            + "      x: T\n"
            + "      y: T\n"
            + "      z: T\n";

    SimpleExtension.ExtensionCollection extensions = SimpleExtension.load(yaml);

    // Create type objects
    Type pointType = Type.UserDefined.builder().nullable(false).urn(urn).name("point").build();

    Type.Parameter pointTypeParam =
        io.substrait.type.ImmutableType.ParameterDataType.builder().type(pointType).build();

    Type vectorOfPointType =
        Type.UserDefined.builder()
            .nullable(false)
            .urn(urn)
            .name("vector")
            .addTypeParameters(pointTypeParam)
            .build();

    // Create literals
    Expression.UserDefinedStructLiteral pointLiteral =
        ExpressionCreator.userDefinedLiteralStruct(
            false,
            urn,
            "point",
            Collections.emptyList(),
            Arrays.asList(ExpressionCreator.i32(false, 10), ExpressionCreator.i32(false, 20)));

    // Create vector<point> literal: vector<point>{(1,2), (3,4), (5,6)}
    Expression.UserDefinedStructLiteral vectorOfPointLiteral =
        ExpressionCreator.userDefinedLiteralStruct(
            false,
            urn,
            "vector",
            Arrays.asList(pointTypeParam),
            Arrays.asList(
                ExpressionCreator.userDefinedLiteralStruct(
                    false,
                    urn,
                    "point",
                    Collections.emptyList(),
                    Arrays.asList(
                        ExpressionCreator.i32(false, 1), ExpressionCreator.i32(false, 2))),
                ExpressionCreator.userDefinedLiteralStruct(
                    false,
                    urn,
                    "point",
                    Collections.emptyList(),
                    Arrays.asList(
                        ExpressionCreator.i32(false, 3), ExpressionCreator.i32(false, 4))),
                ExpressionCreator.userDefinedLiteralStruct(
                    false,
                    urn,
                    "point",
                    Collections.emptyList(),
                    Arrays.asList(
                        ExpressionCreator.i32(false, 5), ExpressionCreator.i32(false, 6)))));

    Type nullablePointType =
        Type.UserDefined.builder().nullable(true).urn(urn).name("point").build();

    Expression.UserDefinedStructLiteral nullablePointLiteral =
        ExpressionCreator.userDefinedLiteralStruct(
            true,
            urn,
            "point",
            Collections.emptyList(),
            Arrays.asList(ExpressionCreator.i32(false, 30), ExpressionCreator.i32(false, 40)));

    // Create virtual table with all three columns (nullable point, required point, required vector)
    VirtualTableScan virtualTable =
        ImmutableVirtualTableScan.builder()
            .initialSchema(
                NamedStruct.of(
                    Arrays.asList("nullable_point_col", "point_col", "vector_col"),
                    TypeCreator.REQUIRED.struct(nullablePointType, pointType, vectorOfPointType)))
            .addRows(
                ExpressionCreator.nestedStruct(
                    false, nullablePointLiteral, pointLiteral, vectorOfPointLiteral))
            .build();

    Plan plan =
        Plan.builder()
            .executionBehavior(defaultExecutionBehavior())
            .addRoots(Root.builder().input(virtualTable).build())
            .build();

    io.substrait.proto.Plan protoPlan = toProtoConverter.toProto(plan);

    assertEquals(1, protoPlan.getExtensionUrnsCount(), "Should have exactly 1 extension URN");
    assertEquals(
        2,
        protoPlan.getExtensionsCount(),
        "Should have exactly 2 type extensions (point and vector), no duplicates");

    ProtoPlanConverter fromProtoConverter = new ProtoPlanConverter(extensions);
    Plan roundTrippedPlan = fromProtoConverter.from(protoPlan);
    assertEquals(plan, roundTrippedPlan, "Plan should roundtrip correctly");
  }

  /**
   * Conversion of Plan with VARIABLE_EVALUATION_MODE_PER_PLAN to protobuf.
   *
   * <p>Verifies that a Plan with ExecutionBehavior set to PER_PLAN mode is correctly converted to
   * protobuf format, including the execution behavior field.
   */
  @Test
  void testToProtoWithExecutionBehaviorPerPlan() {
    // Create a Plan with ExecutionBehavior set to PER_PLAN
    Plan.ExecutionBehavior executionBehavior =
        ImmutableExecutionBehavior.builder()
            .variableEvaluationMode(
                Plan.ExecutionBehavior.VariableEvaluationMode.VARIABLE_EVALUATION_MODE_PER_PLAN)
            .build();

    Plan plan =
        ImmutablePlan.builder()
            .executionBehavior(executionBehavior)
            .roots(Collections.emptyList())
            .expectedTypeUrls(Collections.emptyList())
            .build();

    // Convert to protobuf
    io.substrait.proto.Plan protoPlan = toProtoConverter.toProto(plan);

    // Verify the protobuf has execution behavior
    assertTrue(protoPlan.hasExecutionBehavior(), "Protobuf Plan should have ExecutionBehavior");
    assertEquals(
        io.substrait.proto.ExecutionBehavior.VariableEvaluationMode
            .VARIABLE_EVALUATION_MODE_PER_PLAN,
        protoPlan.getExecutionBehavior().getVariableEvalMode(),
        "Variable evaluation mode should be PER_PLAN");
  }

  /**
   * Conversion of Plan with VARIABLE_EVALUATION_MODE_PER_RECORD to protobuf.
   *
   * <p>Verifies that a Plan with ExecutionBehavior set to PER_RECORD mode is correctly converted to
   * protobuf format.
   */
  @Test
  void testToProtoWithExecutionBehaviorPerRecord() {
    // Create a Plan with ExecutionBehavior set to PER_RECORD
    Plan.ExecutionBehavior executionBehavior =
        ImmutableExecutionBehavior.builder()
            .variableEvaluationMode(
                Plan.ExecutionBehavior.VariableEvaluationMode.VARIABLE_EVALUATION_MODE_PER_RECORD)
            .build();

    Plan plan =
        ImmutablePlan.builder()
            .executionBehavior(executionBehavior)
            .roots(Collections.emptyList())
            .expectedTypeUrls(Collections.emptyList())
            .build();

    // Convert to protobuf
    io.substrait.proto.Plan protoPlan = toProtoConverter.toProto(plan);

    // Verify the protobuf has execution behavior
    assertTrue(protoPlan.hasExecutionBehavior(), "Protobuf Plan should have ExecutionBehavior");
    assertEquals(
        io.substrait.proto.ExecutionBehavior.VariableEvaluationMode
            .VARIABLE_EVALUATION_MODE_PER_RECORD,
        protoPlan.getExecutionBehavior().getVariableEvalMode(),
        "Variable evaluation mode should be PER_RECORD");
  }

  /**
   * Conversion from protobuf Plan with ExecutionBehavior to POJO.
   *
   * <p>Verifies that a protobuf Plan with ExecutionBehavior is correctly converted to the POJO
   * representation.
   */
  @Test
  void testFromProtoWithExecutionBehaviorPerPlan() {
    // Create a protobuf Plan with ExecutionBehavior
    io.substrait.proto.Plan protoPlan =
        io.substrait.proto.Plan.newBuilder()
            .setExecutionBehavior(
                io.substrait.proto.ExecutionBehavior.newBuilder()
                    .setVariableEvalMode(
                        io.substrait.proto.ExecutionBehavior.VariableEvaluationMode
                            .VARIABLE_EVALUATION_MODE_PER_PLAN)
                    .build())
            .build();

    // Convert to POJO
    Plan plan = fromProtoConverter.from(protoPlan);

    // Verify the POJO has execution behavior
    assertTrue(
        plan.getExecutionBehavior().isPresent(), "Plan should have ExecutionBehavior present");
    assertEquals(
        Plan.ExecutionBehavior.VariableEvaluationMode.VARIABLE_EVALUATION_MODE_PER_PLAN,
        plan.getExecutionBehavior().get().getVariableEvaluationMode(),
        "Variable evaluation mode should be PER_PLAN");
  }

  /**
   * Conversion from protobuf Plan with PER_RECORD mode to POJO.
   *
   * <p>Verifies that a protobuf Plan with PER_RECORD execution behavior is correctly converted.
   */
  @Test
  void testFromProtoWithExecutionBehaviorPerRecord() {
    // Create a protobuf Plan with ExecutionBehavior set to PER_RECORD
    io.substrait.proto.Plan protoPlan =
        io.substrait.proto.Plan.newBuilder()
            .setExecutionBehavior(
                io.substrait.proto.ExecutionBehavior.newBuilder()
                    .setVariableEvalMode(
                        io.substrait.proto.ExecutionBehavior.VariableEvaluationMode
                            .VARIABLE_EVALUATION_MODE_PER_RECORD)
                    .build())
            .build();

    // Convert to POJO
    Plan plan = fromProtoConverter.from(protoPlan);

    // Verify the POJO has execution behavior
    assertTrue(
        plan.getExecutionBehavior().isPresent(), "Plan should have ExecutionBehavior present");
    assertEquals(
        Plan.ExecutionBehavior.VariableEvaluationMode.VARIABLE_EVALUATION_MODE_PER_RECORD,
        plan.getExecutionBehavior().get().getVariableEvaluationMode(),
        "Variable evaluation mode should be PER_RECORD");
  }

  /**
   * Conversion from protobuf Plan without ExecutionBehavior.
   *
   * <p>Verifies that a protobuf Plan without ExecutionBehavior results in a POJO Plan that fails
   * validation (since ExecutionBehavior is required).
   */
  @Test
  void testFromProtoWithoutExecutionBehaviorFailsValidation() {
    // Create a protobuf Plan without ExecutionBehavior
    io.substrait.proto.Plan protoPlan = io.substrait.proto.Plan.newBuilder().build();

    // Attempt to convert to POJO - should fail validation
    assertThrows(
        IllegalArgumentException.class,
        () -> fromProtoConverter.from(protoPlan),
        "Conversion should fail when ExecutionBehavior is not set");
  }

  /**
   * Test case 6: Conversion from protobuf Plan with UNSPECIFIED mode fails validation.
   *
   * <p>Verifies that a protobuf Plan with VARIABLE_EVALUATION_MODE_UNSPECIFIED results in a
   * validation failure when converted to POJO.
   */
  @Test
  void testFromProtoWithUnspecifiedModeFailsValidation() {
    // Create a protobuf Plan with UNSPECIFIED mode
    io.substrait.proto.Plan protoPlan =
        io.substrait.proto.Plan.newBuilder()
            .setExecutionBehavior(
                io.substrait.proto.ExecutionBehavior.newBuilder()
                    .setVariableEvalMode(
                        io.substrait.proto.ExecutionBehavior.VariableEvaluationMode
                            .VARIABLE_EVALUATION_MODE_UNSPECIFIED)
                    .build())
            .build();

    // Attempt to convert to POJO - should fail validation
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> fromProtoConverter.from(protoPlan),
            "Conversion should fail when VariableEvaluationMode is UNSPECIFIED");

    assertTrue(
        exception.getMessage().contains("VariableEvaluationMode"),
        "Error message should mention VariableEvaluationMode");
  }

  /**
   * Round-trip conversion with PER_PLAN mode.
   *
   * <p>Verifies that converting a Plan to protobuf and back preserves all data, including the
   * execution behavior with PER_PLAN mode.
   */
  @Test
  void testRoundTripWithExecutionBehaviorPerPlan() {
    // Create original Plan
    Plan.ExecutionBehavior executionBehavior =
        ImmutableExecutionBehavior.builder()
            .variableEvaluationMode(
                Plan.ExecutionBehavior.VariableEvaluationMode.VARIABLE_EVALUATION_MODE_PER_PLAN)
            .build();

    Plan originalPlan =
        ImmutablePlan.builder()
            .executionBehavior(executionBehavior)
            .roots(Collections.emptyList())
            .expectedTypeUrls(Collections.emptyList())
            .build();

    // Convert to protobuf and back
    io.substrait.proto.Plan protoPlan = toProtoConverter.toProto(originalPlan);
    Plan roundTrippedPlan = fromProtoConverter.from(protoPlan);

    // Verify data integrity
    assertTrue(
        roundTrippedPlan.getExecutionBehavior().isPresent(),
        "Round-tripped Plan should have ExecutionBehavior");
    assertEquals(
        Plan.ExecutionBehavior.VariableEvaluationMode.VARIABLE_EVALUATION_MODE_PER_PLAN,
        roundTrippedPlan.getExecutionBehavior().get().getVariableEvaluationMode(),
        "Variable evaluation mode should be preserved");
    assertEquals(
        originalPlan.getRoots().size(),
        roundTrippedPlan.getRoots().size(),
        "Number of roots should be preserved");
    assertEquals(
        originalPlan.getExpectedTypeUrls().size(),
        roundTrippedPlan.getExpectedTypeUrls().size(),
        "Number of expected type URLs should be preserved");
  }

  /**
   * Round-trip conversion with PER_RECORD mode.
   *
   * <p>Verifies that converting a Plan with PER_RECORD mode to protobuf and back preserves the
   * execution behavior correctly.
   */
  @Test
  void testRoundTripWithExecutionBehaviorPerRecord() {
    // Create original Plan
    Plan.ExecutionBehavior executionBehavior =
        ImmutableExecutionBehavior.builder()
            .variableEvaluationMode(
                Plan.ExecutionBehavior.VariableEvaluationMode.VARIABLE_EVALUATION_MODE_PER_RECORD)
            .build();

    Plan originalPlan =
        ImmutablePlan.builder()
            .executionBehavior(executionBehavior)
            .roots(Collections.emptyList())
            .expectedTypeUrls(Collections.emptyList())
            .build();

    // Convert to protobuf and back
    io.substrait.proto.Plan protoPlan = toProtoConverter.toProto(originalPlan);
    Plan roundTrippedPlan = fromProtoConverter.from(protoPlan);

    // Verify data integrity
    assertTrue(
        roundTrippedPlan.getExecutionBehavior().isPresent(),
        "Round-tripped Plan should have ExecutionBehavior");
    assertEquals(
        Plan.ExecutionBehavior.VariableEvaluationMode.VARIABLE_EVALUATION_MODE_PER_RECORD,
        roundTrippedPlan.getExecutionBehavior().get().getVariableEvaluationMode(),
        "Variable evaluation mode should be preserved");
  }

  /**
   * Empty plan with only execution behavior.
   *
   * <p>Verifies that a minimal Plan with only execution behavior (no roots, no type URLs) can be
   * successfully converted.
   */
  @Test
  void testRoundTripEmptyPlanWithExecutionBehavior() {
    // Create minimal Plan
    Plan.ExecutionBehavior executionBehavior =
        ImmutableExecutionBehavior.builder()
            .variableEvaluationMode(
                Plan.ExecutionBehavior.VariableEvaluationMode.VARIABLE_EVALUATION_MODE_PER_PLAN)
            .build();

    Plan originalPlan =
        ImmutablePlan.builder()
            .executionBehavior(executionBehavior)
            .roots(Collections.emptyList())
            .expectedTypeUrls(Collections.emptyList())
            .build();

    // Verify conversion succeeds
    assertDoesNotThrow(
        () -> {
          io.substrait.proto.Plan protoPlan = toProtoConverter.toProto(originalPlan);
          Plan roundTrippedPlan = fromProtoConverter.from(protoPlan);
          assertNotNull(roundTrippedPlan, "Round-tripped Plan should not be null");
        },
        "Empty plan with execution behavior should convert successfully");
  }

  /**
   * Verify that protobuf without execution behavior field is handled.
   *
   * <p>Tests the edge case where a protobuf Plan doesn't have the execution behavior field set at
   * all.
   */
  @Test
  void testFromProtoMissingExecutionBehaviorField() {
    // Create protobuf Plan without setting execution behavior
    io.substrait.proto.Plan protoPlan = io.substrait.proto.Plan.newBuilder().build();

    // Verify hasExecutionBehavior returns false
    assertFalse(
        protoPlan.hasExecutionBehavior(), "Protobuf Plan should not have execution behavior field");

    // Conversion should fail validation
    assertThrows(
        IllegalArgumentException.class,
        () -> fromProtoConverter.from(protoPlan),
        "Conversion should fail when execution behavior is missing");
  }
}
