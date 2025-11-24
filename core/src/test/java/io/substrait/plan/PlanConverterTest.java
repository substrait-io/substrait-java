package io.substrait.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.AdvancedExtension;
import io.substrait.extension.SimpleExtension;
import io.substrait.plan.Plan.Root;
import io.substrait.relation.EmptyScan;
import io.substrait.relation.ImmutableVirtualTableScan;
import io.substrait.relation.VirtualTableScan;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.utils.StringHolder;
import io.substrait.utils.StringHolderHandlingExtensionProtoConverter;
import io.substrait.utils.StringHolderHandlingProtoExtensionConverter;
import java.util.Arrays;
import java.util.Collections;
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

  @Test
  void planIncludingRelationWithAdvancedExtension() {
    final StringHolder enhanced = new StringHolder("ENHANCED");
    final StringHolder optimized = new StringHolder("OPTIMIZED");

    final Plan plan =
        Plan.builder()
            .addRoots(
                Root.builder()
                    .input(
                        EmptyScan.builder()
                            .initialSchema(
                                NamedStruct.builder().struct(TypeCreator.REQUIRED.struct()).build())
                            .extension(
                                AdvancedExtension.builder()
                                    .enhancement(enhanced)
                                    .addOptimizations(optimized)
                                    .build())
                            .build())
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

  /**
   * Verifies that nested UserDefined types with type parameters share the same ExtensionCollector
   * and don't create duplicate type references. Tests that a plan containing both a standalone
   * UserDefined literal (point) and a parameterized UserDefined literal (vector<point>) correctly
   * registers both types in the extension collection without duplication.
   */
  @Test
  void nestedUserDefinedTypesShareExtensionCollector() {
    // Define custom types: point and vector<T>
    String urn = "extension:test:nested_types";
    String yaml =
        "---\n"
            + "urn: "
            + urn
            + "\n"
            + "types:\n"
            + "  - name: point\n"
            + "    structure:\n"
            + "      x: i32\n"
            + "      y: i32\n"
            + "  - name: vector\n"
            + "    parameters:\n"
            + "      - name: T\n"
            + "        type: dataType\n"
            + "    structure:\n"
            + "      x: T\n"
            + "      y: T\n"
            + "      z: T\n";

    SimpleExtension.ExtensionCollection extensions = SimpleExtension.load("test.yaml", yaml);

    // Create type objects
    Type pointType = Type.UserDefined.builder().nullable(false).urn(urn).name("point").build();

    Type.Parameter pointTypeParam =
        io.substrait.type.ImmutableType.ParameterDataType.builder().type(pointType).build();

    Type vectorOfPointType =
        Type.UserDefined.builder()
            .nullable(false)
            .urn(urn)
            .name("vector")
            .addTypeParameters(pointTypeParam)
            .build();

    // Create literals
    Expression.UserDefinedStructLiteral pointLiteral =
        ExpressionCreator.userDefinedLiteralStruct(
            false,
            urn,
            "point",
            Collections.emptyList(),
            Arrays.asList(ExpressionCreator.i32(false, 10), ExpressionCreator.i32(false, 20)));

    // Create vector<point> literal: vector<point>{(1,2), (3,4), (5,6)}
    Expression.UserDefinedStructLiteral vectorOfPointLiteral =
        ExpressionCreator.userDefinedLiteralStruct(
            false,
            urn,
            "vector",
            Arrays.asList(pointTypeParam),
            Arrays.asList(
                ExpressionCreator.userDefinedLiteralStruct(
                    false,
                    urn,
                    "point",
                    Collections.emptyList(),
                    Arrays.asList(
                        ExpressionCreator.i32(false, 1), ExpressionCreator.i32(false, 2))),
                ExpressionCreator.userDefinedLiteralStruct(
                    false,
                    urn,
                    "point",
                    Collections.emptyList(),
                    Arrays.asList(
                        ExpressionCreator.i32(false, 3), ExpressionCreator.i32(false, 4))),
                ExpressionCreator.userDefinedLiteralStruct(
                    false,
                    urn,
                    "point",
                    Collections.emptyList(),
                    Arrays.asList(
                        ExpressionCreator.i32(false, 5), ExpressionCreator.i32(false, 6)))));

    Type nullablePointType =
        Type.UserDefined.builder().nullable(true).urn(urn).name("point").build();

    Expression.UserDefinedStructLiteral nullablePointLiteral =
        ExpressionCreator.userDefinedLiteralStruct(
            true,
            urn,
            "point",
            Collections.emptyList(),
            Arrays.asList(ExpressionCreator.i32(false, 30), ExpressionCreator.i32(false, 40)));

    // Create virtual table with all three columns (nullable point, required point, required vector)
    VirtualTableScan virtualTable =
        ImmutableVirtualTableScan.builder()
            .initialSchema(
                NamedStruct.of(
                    Arrays.asList("nullable_point_col", "point_col", "vector_col"),
                    TypeCreator.REQUIRED.struct(nullablePointType, pointType, vectorOfPointType)))
            .addRows(
                ExpressionCreator.struct(
                    false, nullablePointLiteral, pointLiteral, vectorOfPointLiteral))
            .build();

    Plan plan = Plan.builder().addRoots(Root.builder().input(virtualTable).build()).build();

    PlanProtoConverter toProtoConverter = new PlanProtoConverter();
    io.substrait.proto.Plan protoPlan = toProtoConverter.toProto(plan);

    assertEquals(1, protoPlan.getExtensionUrnsCount(), "Should have exactly 1 extension URN");
    assertEquals(
        2,
        protoPlan.getExtensionsCount(),
        "Should have exactly 2 type extensions (point and vector), no duplicates");

    ProtoPlanConverter fromProtoConverter = new ProtoPlanConverter(extensions);
    Plan roundTrippedPlan = fromProtoConverter.from(protoPlan);
    assertEquals(plan, roundTrippedPlan, "Plan should roundtrip correctly");
  }
}
