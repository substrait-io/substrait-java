package io.substrait.plan;

import io.substrait.expression.proto.NestingDetacher;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.ExtensionProtoConverter;
import io.substrait.extension.SimpleExtension.ExtensionCollection;
import io.substrait.proto.ExecutionBehavior;
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
  /** Converts gathered function and type extensions into their proto representation. */
  @NonNull protected final ExtensionProtoConverter<?, ?> extensionProtoConverter;

  @NonNull private final ExtensionCollection extensionCollection;

  @NonNull private final NestingDetacher nestingDetacher;

  /** The spec release that introduced detached expressions, as major, minor and patch. */
  private static final int[] DETACHED_EXPRESSIONS_SINCE = {0, 105, 0};

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
    this(extensionCollection, extensionProtoConverter, NestingDetacher.defaultDetacher());
  }

  /**
   * Constructor with custom {@link ExtensionCollection}, {@link ExtensionProtoConverter} and {@link
   * NestingDetacher}.
   *
   * <p>The other constructors use {@link NestingDetacher#defaultDetacher()}, which leaves a plan
   * unchanged unless its expressions nest too deeply for protobuf parsers with default limits.
   *
   * @param extensionCollection custom {@link ExtensionCollection} to use, must not be null
   * @param extensionProtoConverter custom {@link ExtensionProtoConverter} to use, must not be null
   * @param nestingDetacher bounds the nesting depth of each {@link PlanRel}, must not be null
   */
  public PlanProtoConverter(
      @NonNull final ExtensionCollection extensionCollection,
      @NonNull final ExtensionProtoConverter<?, ?> extensionProtoConverter,
      @NonNull final NestingDetacher nestingDetacher) {
    if (nestingDetacher == null) {
      throw new IllegalArgumentException("NestingDetacher is required");
    }
    if (extensionCollection == null) {
      throw new IllegalArgumentException("ExtensionCollection is required");
    }
    if (extensionProtoConverter == null) {
      throw new IllegalArgumentException("ExtensionProtoConverter is required");
    }
    this.extensionCollection = extensionCollection;
    this.extensionProtoConverter = extensionProtoConverter;
    this.nestingDetacher = nestingDetacher;
  }

  /**
   * Converts a {@link io.substrait.plan.Plan} object to its protobuf representation.
   *
   * @param plan the Plan object to convert, must not be null
   * @return the protobuf Plan representation
   * @throws IllegalArgumentException if the plan contains invalid data
   */
  public Plan toProto(final io.substrait.plan.Plan plan) {
    final List<PlanRel> planRels = new ArrayList<>();
    boolean detached = false;
    final ExtensionCollector functionCollector = new ExtensionCollector(extensionCollection);
    for (final io.substrait.plan.Plan.Root root : plan.getRoots()) {
      final Rel input =
          new RelProtoConverter(functionCollector, extensionProtoConverter)
              .toProto(root.getInput());
      final PlanRel planRel =
          PlanRel.newBuilder()
              .setRoot(
                  io.substrait.proto.RelRoot.newBuilder()
                      .setInput(input)
                      .addAllNames(root.getNames()))
              .build();
      final PlanRel bounded = nestingDetacher.detach(planRel);
      detached |= bounded != planRel;
      planRels.add(bounded);
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
    if (detached && precedesDetachedExpressions(versionBuilder)) {
      // The git hash identifies the earlier release, so it no longer describes the plan.
      versionBuilder
          .setMajorNumber(DETACHED_EXPRESSIONS_SINCE[0])
          .setMinorNumber(DETACHED_EXPRESSIONS_SINCE[1])
          .setPatchNumber(DETACHED_EXPRESSIONS_SINCE[2])
          .clearGitHash();
    }

    builder.setVersion(versionBuilder);

    // Set execution behavior
    builder.setExecutionBehavior(toProtoExecutionBehavior(plan.getExecutionBehavior()));

    return builder.build();
  }

  /**
   * Whether a plan declaring {@code version} predates detached expressions, which a {@link PlanRel}
   * rewritten by the {@link NestingDetacher} uses, so the declared version has to be raised.
   */
  private static boolean precedesDetachedExpressions(final Version.Builder version) {
    final int[] declared = {
      version.getMajorNumber(), version.getMinorNumber(), version.getPatchNumber()
    };
    for (int i = 0; i < declared.length; i++) {
      if (declared[i] != DETACHED_EXPRESSIONS_SINCE[i]) {
        return declared[i] < DETACHED_EXPRESSIONS_SINCE[i];
      }
    }
    return false;
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
        .setVariableEvalMode(executionBehavior.getVariableEvaluationMode().toProto())
        .build();
  }
}
