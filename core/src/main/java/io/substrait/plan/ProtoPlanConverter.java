package io.substrait.plan;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.ExtensionLookup;
import io.substrait.extension.ImmutableExtensionLookup;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.PlanRel;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Converts from {@link io.substrait.proto.Plan} to {@link io.substrait.plan.Plan} */
public class ProtoPlanConverter {

  protected final SimpleExtension.ExtensionCollection extensionCollection;

  public ProtoPlanConverter() {
    this(DefaultExtensionCatalog.DEFAULT_COLLECTION);
  }

  public ProtoPlanConverter(SimpleExtension.ExtensionCollection extensionCollection) {
    if (extensionCollection == null) {
      throw new IllegalArgumentException("ExtensionCollection is required");
    }
    this.extensionCollection = extensionCollection;
  }

  /** Override hook for providing custom {@link ProtoRelConverter} implementations */
  protected ProtoRelConverter getProtoRelConverter(ExtensionLookup functionLookup) {
    return new ProtoRelConverter(functionLookup, this.extensionCollection);
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
            Optional.ofNullable(plan.hasAdvancedExtensions() ? plan.getAdvancedExtensions() : null))
        .version(versionBuilder.build())
        .build();
  }
}
