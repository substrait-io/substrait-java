package io.substrait.plan;

import io.substrait.extension.ExtensionCollector;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.Rel;
import io.substrait.relation.RelProtoConverter;
import java.util.ArrayList;
import java.util.List;

/** Converts from {@link io.substrait.plan.Plan} to {@link io.substrait.proto.Plan} */
public class PlanProtoConverter {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(PlanProtoConverter.class);

  public Plan toProto(io.substrait.plan.Plan plan) {
    List<PlanRel> planRels = new ArrayList<>();
    ExtensionCollector functionCollector = new ExtensionCollector();
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
      builder.setAdvancedExtensions(plan.getAdvancedExtension().get());
    }
    return builder.build();
  }
}
