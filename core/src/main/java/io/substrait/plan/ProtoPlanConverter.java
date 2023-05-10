package io.substrait.plan;

import io.substrait.expression.ExtensionLookup;
import io.substrait.expression.proto.ImmutableFunctionLookup;
import io.substrait.function.SimpleExtension;
import io.substrait.proto.PlanRel;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Converts from {@link io.substrait.proto.Plan} to {@link io.substrait.plan.Plan} */
public class ProtoPlanConverter {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(io.substrait.plan.ProtoPlanConverter.class);

  protected final SimpleExtension.ExtensionCollection extensionCollection;

  public ProtoPlanConverter() throws IOException {
    this(SimpleExtension.loadDefaults());
  }

  public ProtoPlanConverter(SimpleExtension.ExtensionCollection extensionCollection) {
    this.extensionCollection = extensionCollection;
  }

  /** Override hook for providing custom {@link ProtoRelConverter} implementations */
  protected ProtoRelConverter getProtoRelConverter(ExtensionLookup functionLookup) {
    return new ProtoRelConverter(functionLookup, this.extensionCollection);
  }

  public Plan from(io.substrait.proto.Plan plan) {
    ExtensionLookup functionLookup = ImmutableFunctionLookup.builder().from(plan).build();
    ProtoRelConverter relConverter = getProtoRelConverter(functionLookup);
    List<Plan.Root> roots = new ArrayList<>();
    for (PlanRel planRel : plan.getRelationsList()) {
      io.substrait.proto.RelRoot root = planRel.getRoot();
      Rel rel = relConverter.from(root.getInput());
      roots.add(ImmutableRoot.builder().input(rel).names(root.getNamesList()).build());
    }
    return ImmutablePlan.builder()
        .roots(roots)
        .expectedTypeUrls(plan.getExpectedTypeUrlsList())
        .advancedExtension(
            Optional.ofNullable(plan.hasAdvancedExtensions() ? plan.getAdvancedExtensions() : null))
        .build();
  }
}
