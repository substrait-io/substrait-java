package io.substrait.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.extension.AdvancedExtension;
import io.substrait.utils.StringHolder;
import io.substrait.utils.StringHolderHandlingExtensionProtoConverter;
import io.substrait.utils.StringHolderHandlingProtoExtensionConverter;
import org.junit.jupiter.api.Test;

class PlanConverterTest {
  @Test
  void emptyAdvancedExtensionTest() {
    final Plan plan = Plan.builder().advancedExtension(AdvancedExtension.builder().build()).build();
    final PlanProtoConverter toProtoConverter = new PlanProtoConverter();
    final io.substrait.proto.Plan protoPlan = toProtoConverter.toProto(plan);

    final ProtoPlanConverter fromProtoConverter = new ProtoPlanConverter();
    final Plan plan2 = fromProtoConverter.from(protoPlan);

    assertEquals(plan, plan2);
  }

  @Test
  void enhancementOnlyAdvancedExtension() {
    final StringHolder enhanced = new StringHolder("ENHANCED");

    final Plan plan =
        Plan.builder()
            .advancedExtension(AdvancedExtension.builder().enhancement(enhanced).build())
            .build();
    final PlanProtoConverter toProtoConverter = new PlanProtoConverter();
    final io.substrait.proto.Plan protoPlan = toProtoConverter.toProto(plan);

    final ProtoPlanConverter fromProtoConverter = new ProtoPlanConverter();
    assertThrows(
        UnsupportedOperationException.class,
        () -> fromProtoConverter.from(protoPlan),
        "missing deserialization logic for AdvancedExtension.Enhancement");
  }

  @Test
  void optimizationOnlyAdvancedExtension() {
    final StringHolder optimized = new StringHolder("OPTIMIZED");

    final Plan plan =
        Plan.builder()
            .advancedExtension(AdvancedExtension.builder().addOptimizations(optimized).build())
            .build();
    final PlanProtoConverter toProtoConverter = new PlanProtoConverter();
    final io.substrait.proto.Plan protoPlan = toProtoConverter.toProto(plan);

    // The optimization is serialized correctly to protobuf.
    // When it is read back in, the default ProtoPlanConverter throws UnsupportedOperationException
    // since it missing the logic to deserialize the optimization.

    final ProtoPlanConverter fromProtoConverter = new ProtoPlanConverter();
    assertThrows(
        UnsupportedOperationException.class,
        () -> fromProtoConverter.from(protoPlan),
        "missing deserialization logic for AdvancedExtension.Optimization");
  }

  @Test
  void advancedExtensionWithEnhancementAndOptimization() {
    final StringHolder enhanced = new StringHolder("ENHANCED");
    final StringHolder optimized = new StringHolder("OPTIMIZED");

    final Plan plan =
        Plan.builder()
            .advancedExtension(
                AdvancedExtension.builder()
                    .enhancement(enhanced)
                    .addOptimizations(optimized)
                    .build())
            .build();
    final PlanProtoConverter toProtoConverter = new PlanProtoConverter();
    final io.substrait.proto.Plan protoPlan = toProtoConverter.toProto(plan);

    final ProtoPlanConverter fromProtoConverter = new ProtoPlanConverter();

    // Enhancements are not handled by the default ProtoPlanConverter
    assertThrows(
        UnsupportedOperationException.class,
        () -> fromProtoConverter.from(protoPlan),
        "missing deserialization logic for AdvancedExtension.Enhancement");
  }

  @Test
  void customAdvancedExtensionSerde() {
    final StringHolder enhanced = new StringHolder("ENHANCED");
    final StringHolder optimized = new StringHolder("OPTIMIZED");

    final Plan plan =
        Plan.builder()
            .advancedExtension(
                AdvancedExtension.builder()
                    .enhancement(enhanced)
                    .addOptimizations(optimized)
                    .build())
            .build();
    final PlanProtoConverter toProtoConverter =
        new PlanProtoConverter(new StringHolderHandlingExtensionProtoConverter());
    final io.substrait.proto.Plan protoPlan = toProtoConverter.toProto(plan);

    final ProtoPlanConverter fromProtoConverter =
        new ProtoPlanConverter(new StringHolderHandlingProtoExtensionConverter());
    final Plan plan2 = fromProtoConverter.from(protoPlan);

    assertEquals(plan, plan2);
  }
}
