package io.substrait.plan;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.ExtensionProtoConverter;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.Rel;
import io.substrait.proto.Version;
import io.substrait.relation.RelProtoConverter;
import java.util.ArrayList;
import java.util.List;

/** Converts from {@link io.substrait.plan.Plan} to {@link io.substrait.proto.Plan} */
public class PlanProtoConverter {
  protected final ExtensionProtoConverter extensionProtoConverter;

  private final SimpleExtension.ExtensionCollection extensionCollection;

  public PlanProtoConverter() {
    this(DefaultExtensionCatalog.DEFAULT_COLLECTION);
  }

  public PlanProtoConverter(final SimpleExtension.ExtensionCollection extensionCollection) {
    this(extensionCollection, new ExtensionProtoConverter());
  }

  public PlanProtoConverter(final ExtensionProtoConverter extensionProtoConverter) {
    this(DefaultExtensionCatalog.DEFAULT_COLLECTION, extensionProtoConverter);
  }

  public PlanProtoConverter(
      final SimpleExtension.ExtensionCollection extensionCollection,
      final ExtensionProtoConverter extensionProtoConverter) {
    if (extensionCollection == null) {
      throw new IllegalArgumentException("ExtensionCollection is required");
    }
    this.extensionCollection = extensionCollection;
    this.extensionProtoConverter = extensionProtoConverter;
  }

  public Plan toProto(io.substrait.plan.Plan plan) {
    List<PlanRel> planRels = new ArrayList<>();
    ExtensionCollector functionCollector = new ExtensionCollector(extensionCollection);
    for (io.substrait.plan.Plan.Root root : plan.getRoots()) {
      Rel input = new RelProtoConverter(functionCollector).toProto(root.getInput());
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
