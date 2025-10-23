package io.substrait.plan;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.ExtensionProtoConverter;
import io.substrait.extension.SimpleExtension.ExtensionCollection;
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

  public Plan toProto(io.substrait.plan.Plan plan) {
    List<PlanRel> planRels = new ArrayList<>();
    ExtensionCollector functionCollector = new ExtensionCollector(extensionCollection);
    for (io.substrait.plan.Plan.Root root : plan.getRoots()) {
      Rel input =
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
    Plan.Builder builder =
        Plan.newBuilder()
            .addAllRelations(planRels)
            .addAllExpectedTypeUrls(plan.getExpectedTypeUrls());
    functionCollector.addExtensionsToPlan(builder);
    if (plan.getAdvancedExtension().isPresent()) {
      builder.setAdvancedExtensions(
          extensionProtoConverter.toProto(plan.getAdvancedExtension().get()));
    }

    Version.Builder versionBuilder =
        Version.newBuilder()
            .setMajorNumber(plan.getVersion().getMajor())
            .setMinorNumber(plan.getVersion().getMinor())
            .setPatchNumber(plan.getVersion().getPatch());

    plan.getVersion().getGitHash().ifPresent(gh -> versionBuilder.setGitHash(gh));
    plan.getVersion().getProducer().ifPresent(p -> versionBuilder.setProducer(p));

    builder.setVersion(versionBuilder);

    return builder.build();
  }
}
