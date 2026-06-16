package io.substrait.plan;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.ExtensionLookup;
import io.substrait.extension.ImmutableExtensionLookup;
import io.substrait.extension.ProtoExtensionConverter;
import io.substrait.extension.SimpleExtension.ExtensionCollection;
import io.substrait.plan.Plan.ExecutionBehavior;
import io.substrait.plan.Plan.ExecutionBehavior.VariableEvaluationMode;
import io.substrait.proto.PlanRel;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.jspecify.annotations.NonNull;

/** Converts from {@link io.substrait.proto.Plan} to {@link io.substrait.plan.Plan} */
public class ProtoPlanConverter {
  /** Collection of extensions used to resolve function and type references. */
  @NonNull protected final ExtensionCollection extensionCollection;

  /** Converts advanced extension and extension URN information from proto. */
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

  /**
   * Override hook for providing custom {@link ProtoRelConverter} implementations.
   *
   * @param functionLookup lookup used to resolve function references while converting relations
   * @return the {@link ProtoRelConverter} to use
   */
  protected ProtoRelConverter getProtoRelConverter(final ExtensionLookup functionLookup) {
    return new ProtoRelConverter(functionLookup, this.extensionCollection, protoExtensionConverter);
  }

  /**
   * Converts a protobuf {@link io.substrait.proto.Plan} to a {@link Plan} POJO.
   *
   * <p><b>Note:</b> Execution behavior is optional in the protobuf message, but the {@link Plan}
   * POJO requires it. Conversion handles a missing execution behavior based on the plan version:
   *
   * <p><b>Note:</b> If {@code Plan.ExecutionBehavior.VariableEvaluationMode} is not set, it will be
   * defaulted to {@code VARIABLE_EVALUATION_MODE_PER_PLAN}. Once other producers populate this
   * field correctly, this compatibility workaround will be removed.
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

    // Set execution behavior (required field)
    if (plan.hasExecutionBehavior()) {
      planBuilder.executionBehavior(fromProtoExecutionBehavior(plan.getExecutionBehavior()));
    } else {
      // Set default ExecutionBehavior for older plans that don't have it
      planBuilder.executionBehavior(
          ExecutionBehavior.builder()
              .variableEvaluationMode(VariableEvaluationMode.PER_PLAN)
              .build());
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
        return io.substrait.plan.Plan.ExecutionBehavior.VariableEvaluationMode.UNSPECIFIED;
      case VARIABLE_EVALUATION_MODE_PER_PLAN:
        return io.substrait.plan.Plan.ExecutionBehavior.VariableEvaluationMode.PER_PLAN;
      case VARIABLE_EVALUATION_MODE_PER_RECORD:
        return io.substrait.plan.Plan.ExecutionBehavior.VariableEvaluationMode.PER_RECORD;
      case UNRECOGNIZED:
      default:
        throw new IllegalArgumentException("Unknown VariableEvaluationMode: " + mode);
    }
  }
}
