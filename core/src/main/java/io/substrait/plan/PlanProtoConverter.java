package io.substrait.plan;

import io.substrait.expression.proto.FunctionCollector;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.Rel;
import io.substrait.relation.RelProtoConverter;
import java.util.ArrayList;
import java.util.List;

public class PlanProtoConverter {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(PlanProtoConverter.class);

  public Plan toProto(io.substrait.plan.Plan plan) {
    List<PlanRel> planRels = new ArrayList<>();
    for (io.substrait.plan.Plan.Root root : plan.getRoots()) {
      Rel input = new RelProtoConverter(new FunctionCollector()).toProto(root.getInput());
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
            .addAllExtensionUris(plan.getExtensionUris())
            .addAllExtensions(plan.getExtensionDeclarations())
            .addAllExpectedTypeUrls(plan.getExpectedTypeUrls())
            .addAllRelations(planRels);
    if (plan.getAdvancedExtension().isPresent()) {
      builder.setAdvancedExtensions(plan.getAdvancedExtension().get());
    }
    return builder.build();
  }
}
