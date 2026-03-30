package io.substrait.plan;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.ExtensionLookup;
import io.substrait.extension.ImmutableExtensionLookup;
import io.substrait.extension.ProtoExtensionConverter;
import io.substrait.extension.SimpleExtension.ExtensionCollection;
import io.substrait.proto.PlanRel;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.jspecify.annotations.NonNull;

/** Converts from {@link io.substrait.proto.Plan} to {@link io.substrait.plan.Plan} */
public class ProtoPlanConverter {
  @NonNull protected final ExtensionCollection extensionCollection;

  @NonNull protected final ProtoExtensionConverter protoExtensionConverter;

  /** Default constructor. */
  public ProtoPlanConverter() {
    this(DefaultExtensionCatalog.DEFAULT_COLLECTION);
  }

  /**
   * Constructor with a custom {@link ExtensionCollection}.
   *
   * @param extensionCollection custom {@link ExtensionCollection} to use, must not be null
   */
  public ProtoPlanConverter(@NonNull final ExtensionCollection extensionCollection) {
    this(extensionCollection, new ProtoExtensionConverter());
  }

  /**
   * Constructor with a custom {@link ProtoExtensionConverter}.
   *
   * @param protoExtensionConverter custom {@link ProtoExtensionConverter} to use, must not be null
   */
  public ProtoPlanConverter(@NonNull final ProtoExtensionConverter protoExtensionConverter) {
    this(DefaultExtensionCatalog.DEFAULT_COLLECTION, protoExtensionConverter);
  }

  /**
   * Constructor with custom {@link ExtensionCollection} and {@link ProtoExtensionConverter}.
   *
   * @param extensionCollection custom {@link ExtensionCollection} to use, must not be null
   * @param protoExtensionConverter custom {@link ProtoExtensionConverter} to use, must not be null
   */
  public ProtoPlanConverter(
      @NonNull final ExtensionCollection extensionCollection,
      @NonNull final ProtoExtensionConverter protoExtensionConverter) {
    if (extensionCollection == null) {
      throw new IllegalArgumentException("ExtensionCollection is required");
    }
    if (protoExtensionConverter == null) {
      throw new IllegalArgumentException("ProtoExtensionConverter is required");
    }
    this.extensionCollection = extensionCollection;
    this.protoExtensionConverter = protoExtensionConverter;
  }

  /** Override hook for providing custom {@link ProtoRelConverter} implementations */
  protected ProtoRelConverter getProtoRelConverter(final ExtensionLookup functionLookup) {
    return new ProtoRelConverter(functionLookup, this.extensionCollection, protoExtensionConverter);
  }

  /**
   * Converts a protobuf {@link io.substrait.proto.Plan} to a {@link Plan} POJO.
   *
   * <p>This method performs a complete conversion from the protobuf representation, including:
   *
   * <ul>
   *   <li>All plan relations and their roots
   *   <li>Extension functions and types lookup
   *   <li>Version information (major, minor, patch, git hash, producer)
   *   <li>Advanced extensions if present
   *   <li>Execution behavior configuration if present
   * </ul>
   *
   * <p><b>Note:</b> While execution behavior is optional in the protobuf message, the Plan
   * validation will still fail if ExecutionBehavior is not set or if it contains
   * VARIABLE_EVALUATION_MODE_UNSPECIFIED. Ensure the protobuf Plan has a valid execution behavior
   * before conversion.
   *
   * @param plan the protobuf Plan to convert, must not be null
   * @return the converted Plan POJO
   * @throws IllegalArgumentException if the plan contains invalid data or if the execution behavior
   *     validation fails
   */
  public Plan from(io.substrait.proto.Plan plan) {
    ExtensionLookup functionLookup = ImmutableExtensionLookup.builder().from(plan).build();
    ProtoRelConverter relConverter = getProtoRelConverter(functionLookup);
    List<Plan.Root> roots = new ArrayList<>();
    for (PlanRel planRel : plan.getRelationsList()) {
      io.substrait.proto.RelRoot root = planRel.getRoot();
      Rel rel = relConverter.from(root.getInput());
      roots.add(Plan.Root.builder().input(rel).names(root.getNamesList()).build());
    }

    ImmutableVersion.Builder versionBuilder =
        ImmutableVersion.builder()
            .major(plan.getVersion().getMajorNumber())
            .minor(plan.getVersion().getMinorNumber())
            .patch(plan.getVersion().getPatchNumber());

    // protobuf field 'git_hash' is an empty string by default
    if (!plan.getVersion().getGitHash().isEmpty()) {
      versionBuilder.gitHash(Optional.of(plan.getVersion().getGitHash()));
    }

    // protobuf field 'producer' is an empty string by default
    if (!plan.getVersion().getProducer().isEmpty()) {
      versionBuilder.producer(Optional.of(plan.getVersion().getProducer()));
    }

    ImmutablePlan.Builder planBuilder =
        Plan.builder()
            .roots(roots)
            .expectedTypeUrls(plan.getExpectedTypeUrlsList())
            .advancedExtension(
                Optional.ofNullable(
                    plan.hasAdvancedExtensions()
                        ? protoExtensionConverter.fromProto(plan.getAdvancedExtensions())
                        : null))
            .version(versionBuilder.build());

    // Set execution behavior if present
    if (plan.hasExecutionBehavior()) {
      planBuilder.executionBehavior(fromProtoExecutionBehavior(plan.getExecutionBehavior()));
    }

    return planBuilder.build();
  }

  /**
   * Converts a protobuf {@link io.substrait.proto.ExecutionBehavior} to its POJO representation.
   *
   * <p>This method converts the execution behavior configuration from the protobuf format to the
   * POJO representation, including the variable evaluation mode.
   *
   * @param executionBehavior the protobuf ExecutionBehavior to convert, must not be null
   * @return the POJO ExecutionBehavior representation
   * @throws IllegalArgumentException if the variable evaluation mode is unknown or UNRECOGNIZED
   */
  private io.substrait.plan.Plan.ExecutionBehavior fromProtoExecutionBehavior(
      final io.substrait.proto.ExecutionBehavior executionBehavior) {
    return io.substrait.plan.Plan.ExecutionBehavior.builder()
        .variableEvaluationMode(
            fromProtoVariableEvaluationMode(executionBehavior.getVariableEvalMode()))
        .build();
  }

  /**
   * Converts a protobuf {@link io.substrait.proto.ExecutionBehavior.VariableEvaluationMode} to its
   * POJO representation.
   *
   * <p>Supported modes:
   *
   * <ul>
   *   <li>{@link
   *       io.substrait.proto.ExecutionBehavior.VariableEvaluationMode#VARIABLE_EVALUATION_MODE_UNSPECIFIED}
   *       - Unspecified mode (will cause validation failure in Plan)
   *   <li>{@link
   *       io.substrait.proto.ExecutionBehavior.VariableEvaluationMode#VARIABLE_EVALUATION_MODE_PER_PLAN}
   *       - Variables are evaluated once per plan execution
   *   <li>{@link
   *       io.substrait.proto.ExecutionBehavior.VariableEvaluationMode#VARIABLE_EVALUATION_MODE_PER_RECORD}
   *       - Variables are evaluated for each record
   * </ul>
   *
   * @param mode the protobuf VariableEvaluationMode to convert, must not be null
   * @return the POJO VariableEvaluationMode representation
   * @throws IllegalArgumentException if the mode is UNRECOGNIZED or not supported
   */
  private io.substrait.plan.Plan.ExecutionBehavior.VariableEvaluationMode
      fromProtoVariableEvaluationMode(
          final io.substrait.proto.ExecutionBehavior.VariableEvaluationMode mode) {
    switch (mode) {
      case VARIABLE_EVALUATION_MODE_UNSPECIFIED:
        return io.substrait.plan.Plan.ExecutionBehavior.VariableEvaluationMode
            .VARIABLE_EVALUATION_MODE_UNSPECIFIED;
      case VARIABLE_EVALUATION_MODE_PER_PLAN:
        return io.substrait.plan.Plan.ExecutionBehavior.VariableEvaluationMode
            .VARIABLE_EVALUATION_MODE_PER_PLAN;
      case VARIABLE_EVALUATION_MODE_PER_RECORD:
        return io.substrait.plan.Plan.ExecutionBehavior.VariableEvaluationMode
            .VARIABLE_EVALUATION_MODE_PER_RECORD;
      case UNRECOGNIZED:
      default:
        throw new IllegalArgumentException("Unknown VariableEvaluationMode: " + mode);
    }
  }
}
