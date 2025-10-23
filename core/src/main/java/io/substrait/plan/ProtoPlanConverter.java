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
  protected ProtoRelConverter getProtoRelConverter(ExtensionLookup functionLookup) {
    return new ProtoRelConverter(functionLookup, this.extensionCollection, protoExtensionConverter);
  }

  public Plan from(io.substrait.proto.Plan plan) {
    ExtensionLookup functionLookup =
        ImmutableExtensionLookup.builder(extensionCollection).from(plan).build();
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

    return Plan.builder()
        .roots(roots)
        .expectedTypeUrls(plan.getExpectedTypeUrlsList())
        .advancedExtension(
            Optional.ofNullable(
                plan.hasAdvancedExtensions()
                    ? protoExtensionConverter.fromProto(plan.getAdvancedExtensions())
                    : null))
        .version(versionBuilder.build())
        .build();
  }
}
