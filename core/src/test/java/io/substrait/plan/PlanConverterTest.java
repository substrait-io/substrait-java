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
  void enhancementOnlyAdvancedExtensionWithoutExtensionProtoConverter() {
    final StringHolder enhanced = new StringHolder("ENHANCED");

    final Plan plan =
        Plan.builder()
            .advancedExtension(AdvancedExtension.builder().enhancement(enhanced).build())
            .build();
    final PlanProtoConverter toProtoConverter = new PlanProtoConverter();

    assertThrows(
        UnsupportedOperationException.class,
        () -> toProtoConverter.toProto(plan),
        "missing serialization logic for AdvancedExtension.Enhancement");
  }

  @Test
  void enhancementOnlyAdvancedExtensionWithExtensionProtoConverter() {
    final StringHolder enhanced = new StringHolder("ENHANCED");

    final Plan plan =
        Plan.builder()
            .advancedExtension(AdvancedExtension.builder().enhancement(enhanced).build())
            .build();
    final PlanProtoConverter toProtoConverter =
        new PlanProtoConverter(new StringHolderHandlingExtensionProtoConverter());
    final io.substrait.proto.Plan protoPlan = toProtoConverter.toProto(plan);

    final ProtoPlanConverter fromProtoConverter = new ProtoPlanConverter();
    assertThrows(
        UnsupportedOperationException.class,
        () -> fromProtoConverter.from(protoPlan),
        "missing deserialization logic for AdvancedExtension.Enhancement");
  }

  @Test
  void enhancementOnlyAdvancedExtensionWithExtensionProtoConverterAndProtoExtensionConverter() {
    final StringHolder enhanced = new StringHolder("ENHANCED");

    final Plan plan =
        Plan.builder()
            .advancedExtension(AdvancedExtension.builder().enhancement(enhanced).build())
            .build();
    final PlanProtoConverter toProtoConverter =
        new PlanProtoConverter(new StringHolderHandlingExtensionProtoConverter());
    final io.substrait.proto.Plan protoPlan = toProtoConverter.toProto(plan);

    final ProtoPlanConverter fromProtoConverter =
        new ProtoPlanConverter(new StringHolderHandlingProtoExtensionConverter());
    final Plan plan2 = fromProtoConverter.from(protoPlan);

    assertEquals(plan, plan2);
  }

  @Test
  void optimizationOnlyAdvancedExtensionWithoutExtensionProtoConverter() {
    final StringHolder optimized = new StringHolder("OPTIMIZED");

    final Plan plan =
        Plan.builder()
            .advancedExtension(AdvancedExtension.builder().addOptimizations(optimized).build())
            .build();
    final PlanProtoConverter toProtoConverter = new PlanProtoConverter();

    assertThrows(
        UnsupportedOperationException.class,
        () -> toProtoConverter.toProto(plan),
        "missing sserialization logic for AdvancedExtension.Optimization");
  }

  @Test
  void optimizationOnlyAdvancedExtensionWithExtensionProtoConverter() {
    final StringHolder optimized = new StringHolder("OPTIMIZED");

    final Plan plan =
        Plan.builder()
            .advancedExtension(AdvancedExtension.builder().addOptimizations(optimized).build())
            .build();
    final PlanProtoConverter toProtoConverter =
        new PlanProtoConverter(new StringHolderHandlingExtensionProtoConverter());
    final io.substrait.proto.Plan protoPlan = toProtoConverter.toProto(plan);

    final ProtoPlanConverter fromProtoConverter = new ProtoPlanConverter();
    assertThrows(
        UnsupportedOperationException.class,
        () -> fromProtoConverter.from(protoPlan),
        "missing deserialization logic for AdvancedExtension.Optimization");
  }

  @Test
  void optimizationOnlyAdvancedExtensionWithExtensionProtoConverterAndProtoExtensionConverter() {
    final StringHolder optimized = new StringHolder("OPTIMIZED");

    final Plan plan =
        Plan.builder()
            .advancedExtension(AdvancedExtension.builder().addOptimizations(optimized).build())
            .build();
    final PlanProtoConverter toProtoConverter =
        new PlanProtoConverter(new StringHolderHandlingExtensionProtoConverter());
    final io.substrait.proto.Plan protoPlan = toProtoConverter.toProto(plan);

    final ProtoPlanConverter fromProtoConverter =
        new ProtoPlanConverter(new StringHolderHandlingProtoExtensionConverter());
    final Plan plan2 = fromProtoConverter.from(protoPlan);

    assertEquals(plan, plan2);
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
    final PlanProtoConverter toProtoConverter =
        new PlanProtoConverter(new StringHolderHandlingExtensionProtoConverter());
    final io.substrait.proto.Plan protoPlan = toProtoConverter.toProto(plan);

    final ProtoPlanConverter fromProtoConverter =
        new ProtoPlanConverter(new StringHolderHandlingProtoExtensionConverter());
    final Plan plan2 = fromProtoConverter.from(protoPlan);

    assertEquals(plan, plan2);
  }
}
