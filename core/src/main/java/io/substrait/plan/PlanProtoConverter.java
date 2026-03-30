package io.substrait.plan;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.ExtensionProtoConverter;
import io.substrait.extension.SimpleExtension.ExtensionCollection;
import io.substrait.proto.ExecutionBehavior;
import io.substrait.proto.ExecutionBehavior.VariableEvaluationMode;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.Rel;
import io.substrait.proto.Version;
import io.substrait.relation.RelProtoConverter;
import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.NonNull;

/** Converts from {@link io.substrait.plan.Plan} to {@link io.substrait.proto.Plan} */
public class PlanProtoConverter {
  @NonNull protected final ExtensionProtoConverter<?, ?> extensionProtoConverter;

  @NonNull private final ExtensionCollection extensionCollection;

  /** Default constructor. */
  public PlanProtoConverter() {
    this(DefaultExtensionCatalog.DEFAULT_COLLECTION);
  }

  /**
   * Constructor with a custom {@link ExtensionCollection}.
   *
   * @param extensionCollection custom {@link ExtensionCollection} to use, must not be null
   */
  public PlanProtoConverter(@NonNull final ExtensionCollection extensionCollection) {
    this(extensionCollection, new ExtensionProtoConverter<>());
  }

  /**
   * Constructor with a custom {@link ExtensionProtoConverter}.
   *
   * @param extensionProtoConverter custom {@link ExtensionProtoConverter} to use, must not be null
   */
  public PlanProtoConverter(@NonNull final ExtensionProtoConverter<?, ?> extensionProtoConverter) {
    this(DefaultExtensionCatalog.DEFAULT_COLLECTION, extensionProtoConverter);
  }

  /**
   * Constructor with custom {@link ExtensionCollection} and {@link ExtensionProtoConverter}.
   *
   * @param extensionCollection custom {@link ExtensionCollection} to use, must not be null
   * @param extensionProtoConverter custom {@link ExtensionProtoConverter} to use, must not be null
   */
  public PlanProtoConverter(
      @NonNull final ExtensionCollection extensionCollection,
      @NonNull final ExtensionProtoConverter<?, ?> extensionProtoConverter) {
    if (extensionCollection == null) {
      throw new IllegalArgumentException("ExtensionCollection is required");
    }
    if (extensionProtoConverter == null) {
      throw new IllegalArgumentException("ExtensionProtoConverter is required");
    }
    this.extensionCollection = extensionCollection;
    this.extensionProtoConverter = extensionProtoConverter;
  }

  /**
   * Converts a {@link io.substrait.plan.Plan} object to its protobuf representation.
   *
   * <p>This method performs a complete conversion of the Plan object, including:
   *
   * <ul>
   *   <li>All plan roots and their associated relations
   *   <li>Extension functions and types used in the plan
   *   <li>Version information
   *   <li>Advanced extensions if present
   *   <li>Execution behavior configuration if present
   * </ul>
   *
   * <p>The execution behavior is optional. If present, it will be converted using {@link
   * #toProtoExecutionBehavior(io.substrait.plan.Plan.ExecutionBehavior)}.
   *
   * @param plan the Plan object to convert, must not be null
   * @return the protobuf Plan representation
   * @throws IllegalArgumentException if the plan contains invalid data
   */
  public Plan toProto(final io.substrait.plan.Plan plan) {
    final List<PlanRel> planRels = new ArrayList<>();
    final ExtensionCollector functionCollector = new ExtensionCollector(extensionCollection);
    for (final io.substrait.plan.Plan.Root root : plan.getRoots()) {
      final Rel input =
          new RelProtoConverter(functionCollector, extensionProtoConverter)
              .toProto(root.getInput());
      planRels.add(
          PlanRel.newBuilder()
              .setRoot(
                  io.substrait.proto.RelRoot.newBuilder()
                      .setInput(input)
                      .addAllNames(root.getNames()))
              .build());
    }
    final Plan.Builder builder =
        Plan.newBuilder()
            .addAllRelations(planRels)
            .addAllExpectedTypeUrls(plan.getExpectedTypeUrls());
    functionCollector.addExtensionsToPlan(builder);
    if (plan.getAdvancedExtension().isPresent()) {
      builder.setAdvancedExtensions(
          extensionProtoConverter.toProto(plan.getAdvancedExtension().get()));
    }

    final Version.Builder versionBuilder =
        Version.newBuilder()
            .setMajorNumber(plan.getVersion().getMajor())
            .setMinorNumber(plan.getVersion().getMinor())
            .setPatchNumber(plan.getVersion().getPatch());

    plan.getVersion().getGitHash().ifPresent(gh -> versionBuilder.setGitHash(gh));
    plan.getVersion().getProducer().ifPresent(p -> versionBuilder.setProducer(p));

    builder.setVersion(versionBuilder);

    // Set execution behavior if present
    if (plan.getExecutionBehavior().isPresent()) {
      builder.setExecutionBehavior(toProtoExecutionBehavior(plan.getExecutionBehavior().get()));
    }

    return builder.build();
  }

  /**
   * Converts an {@link io.substrait.plan.Plan.ExecutionBehavior} to its protobuf representation.
   *
   * <p>This method converts the execution behavior configuration, including the variable evaluation
   * mode, from the POJO representation to the protobuf format.
   *
   * @param executionBehavior the ExecutionBehavior to convert, must not be null
   * @return the protobuf ExecutionBehavior representation
   * @throws IllegalArgumentException if the variable evaluation mode is unknown
   */
  private ExecutionBehavior toProtoExecutionBehavior(
      final io.substrait.plan.Plan.ExecutionBehavior executionBehavior) {
    return ExecutionBehavior.newBuilder()
        .setVariableEvalMode(
            toProtoVariableEvaluationMode(executionBehavior.getVariableEvaluationMode()))
        .build();
  }

  /**
   * Converts a {@link io.substrait.plan.Plan.ExecutionBehavior.VariableEvaluationMode} to its
   * protobuf representation.
   *
   * <p>Supported modes:
   *
   * <ul>
   *   <li>{@link
   *       io.substrait.plan.Plan.ExecutionBehavior.VariableEvaluationMode#VARIABLE_EVALUATION_MODE_UNSPECIFIED}
   *       - Unspecified mode (should be avoided in valid plans)
   *   <li>{@link
   *       io.substrait.plan.Plan.ExecutionBehavior.VariableEvaluationMode#VARIABLE_EVALUATION_MODE_PER_PLAN}
   *       - Variables are evaluated once per plan execution
   *   <li>{@link
   *       io.substrait.plan.Plan.ExecutionBehavior.VariableEvaluationMode#VARIABLE_EVALUATION_MODE_PER_RECORD}
   *       - Variables are evaluated for each record
   * </ul>
   *
   * @param mode the VariableEvaluationMode to convert, must not be null
   * @return the protobuf VariableEvaluationMode representation
   * @throws IllegalArgumentException if the mode is unknown or not supported
   */
  private VariableEvaluationMode toProtoVariableEvaluationMode(
      final io.substrait.plan.Plan.ExecutionBehavior.VariableEvaluationMode mode) {
    switch (mode) {
      case VARIABLE_EVALUATION_MODE_UNSPECIFIED:
        return VariableEvaluationMode.VARIABLE_EVALUATION_MODE_UNSPECIFIED;
      case VARIABLE_EVALUATION_MODE_PER_PLAN:
        return VariableEvaluationMode.VARIABLE_EVALUATION_MODE_PER_PLAN;
      case VARIABLE_EVALUATION_MODE_PER_RECORD:
        return VariableEvaluationMode.VARIABLE_EVALUATION_MODE_PER_RECORD;
      default:
        throw new IllegalArgumentException("Unknown VariableEvaluationMode: " + mode);
    }
  }
}
