package io.substrait.extension;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.util.JsonFormat;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.proto.Plan;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests demonstrating the full URI/URN migration workflow: 1. Consume plans with mixed
 * URI/URN references 2. Convert proto -> POJO using ImmutableExtensionLookup with URI/URN mapping
 * 3. Convert POJO -> proto using PlanProtoConverter 4. Verify output contains proper extension
 * information
 */
public class UriUrnMigrationEndToEndTest {

  private final SimpleExtension.ExtensionCollection defaultExtensions =
      SimpleExtension.loadDefaults();

  /** Load a proto Plan from a JSON resource file using JsonFormat */
  private Plan loadPlanFromJson(String resourcePath) throws IOException {
    try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
      if (inputStream == null) {
        throw new IOException("Resource not found: " + resourcePath);
      }

      String jsonContent =
          new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
              .lines()
              .collect(Collectors.joining("\n"));

      Plan.Builder planBuilder = Plan.newBuilder();
      JsonFormat.parser().merge(jsonContent, planBuilder);
      return planBuilder.build();
    }
  }

  @Test
  public void testUriOnlyToUriUrnMigration() throws IOException {
    // Test with proto plan that has ONLY URI information (no URN)
    BidiMap<String, String> uriUrnMap = new BidiMap<>();
    uriUrnMap.put(
        "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml",
        "extension:io.substrait:functions_arithmetic");

    // 1. LOAD INPUT PROTO PLAN FROM JSON (URI-only)
    Plan originalProtoPlan = loadPlanFromJson("uri-urn-migration/uri-only-input-plan.json");

    // Verify original plan has only URI information
    assertEquals(1, originalProtoPlan.getExtensionUrisCount());
    assertEquals(0, originalProtoPlan.getExtensionUrnsCount()); // No URNs initially

    // 2. ROUNDTRIP: PROTO -> POJO -> PROTO
    SimpleExtension.ExtensionCollection customExtensionCollection =
        SimpleExtension.ExtensionCollection.builder()
            .from(defaultExtensions)
            .uriUrnMap(uriUrnMap)
            .build();
    ProtoPlanConverter protoToPojo = new ProtoPlanConverter(customExtensionCollection);
    io.substrait.plan.Plan pojoPlan = protoToPojo.from(originalProtoPlan);

    PlanProtoConverter pojoToProto = new PlanProtoConverter(customExtensionCollection);
    Plan finalProtoPlan = pojoToProto.toProto(pojoPlan);

    // 3. LOAD EXPECTED PROTO PLAN FROM JSON (should have both URI and URN)
    Plan expectedProtoPlan = loadPlanFromJson("uri-urn-migration/uri-only-expected-plan.json");

    // 4. SIMPLE EQUALITY CHECK
    assertEquals(expectedProtoPlan, finalProtoPlan);
  }

  @Test
  public void testComplexMultiFunctionMigration() throws IOException {
    // Complex test with multiple functions using different reference types
    BidiMap<String, String> uriUrnMap = new BidiMap<>();
    uriUrnMap.put(
        "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml",
        "extension:io.substrait:functions_arithmetic");
    uriUrnMap.put(
        "https://github.com/substrait-io/substrait/blob/main/extensions/functions_string.yaml",
        "extension:io.substrait:functions_string");

    // 1. LOAD INPUT PROTO PLAN FROM JSON
    Plan originalProtoPlan = loadPlanFromJson("uri-urn-migration/complex-input-plan.json");

    // 2. ROUNDTRIP: PROTO -> POJO -> PROTO
    SimpleExtension.ExtensionCollection customExtensionCollection =
        SimpleExtension.ExtensionCollection.builder()
            .from(defaultExtensions)
            .uriUrnMap(uriUrnMap)
            .build();
    ProtoPlanConverter protoToPojo = new ProtoPlanConverter(customExtensionCollection);
    io.substrait.plan.Plan pojoPlan = protoToPojo.from(originalProtoPlan);

    PlanProtoConverter pojoToProto = new PlanProtoConverter(customExtensionCollection);
    Plan finalProtoPlan = pojoToProto.toProto(pojoPlan);

    // 3. LOAD EXPECTED PROTO PLAN FROM JSON
    Plan expectedProtoPlan = loadPlanFromJson("uri-urn-migration/complex-expected-plan.json");

    // 4. SIMPLE EQUALITY CHECK
    assertEquals(expectedProtoPlan, finalProtoPlan);
  }

  @Test
  public void testUrnOnlyArithmeticMigration() throws IOException {
    // Test with proto plan that has ONLY URN information (no URI) - using only arithmetic functions
    BidiMap<String, String> uriUrnMap = new BidiMap<>();
    uriUrnMap.put(
        "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml",
        "extension:io.substrait:functions_arithmetic");

    // 1. LOAD INPUT PROTO PLAN FROM JSON (URN-only, arithmetic functions only)
    Plan originalProtoPlan = loadPlanFromJson("uri-urn-migration/urn-only-input-plan.json");

    // Verify original plan has only URN information
    assertEquals(0, originalProtoPlan.getExtensionUrisCount()); // No URIs initially
    assertEquals(1, originalProtoPlan.getExtensionUrnsCount());

    // 2. ROUNDTRIP: PROTO -> POJO -> PROTO
    SimpleExtension.ExtensionCollection customExtensionCollection =
        SimpleExtension.ExtensionCollection.builder()
            .from(defaultExtensions)
            .uriUrnMap(uriUrnMap)
            .build();
    ProtoPlanConverter protoToPojo = new ProtoPlanConverter(customExtensionCollection);
    io.substrait.plan.Plan pojoPlan = protoToPojo.from(originalProtoPlan);

    PlanProtoConverter pojoToProto = new PlanProtoConverter(customExtensionCollection);
    Plan finalProtoPlan = pojoToProto.toProto(pojoPlan);

    // 3. LOAD EXPECTED PROTO PLAN FROM JSON (should have both URI and URN)
    Plan expectedProtoPlan = loadPlanFromJson("uri-urn-migration/urn-only-expected-plan.json");

    // 4. SIMPLE EQUALITY CHECK
    assertEquals(expectedProtoPlan, finalProtoPlan);
  }

  @Test
  public void testArithmeticOnlyMixedReferences() throws IOException {
    // Test with mixed URI/URN references using only arithmetic functions
    // This demonstrates different ways the same arithmetic functions can be referenced
    BidiMap<String, String> uriUrnMap = new BidiMap<>();
    uriUrnMap.put(
        "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml",
        "extension:io.substrait:functions_arithmetic");

    // 1. LOAD INPUT PROTO PLAN FROM JSON
    Plan originalProtoPlan =
        loadPlanFromJson("uri-urn-migration/mixed-partial-coverage-input-plan.json");

    // Verify input has arithmetic functions with different reference patterns
    assertEquals(1, originalProtoPlan.getExtensionUrisCount()); // 1 URI
    assertEquals(0, originalProtoPlan.getExtensionUrnsCount()); // 0 URN initially
    assertEquals(2, originalProtoPlan.getExtensionsCount()); // 2 arithmetic functions

    // 2. ROUNDTRIP: PROTO -> POJO -> PROTO
    SimpleExtension.ExtensionCollection customExtensionCollection =
        SimpleExtension.ExtensionCollection.builder()
            .from(defaultExtensions)
            .uriUrnMap(uriUrnMap)
            .build();
    ProtoPlanConverter protoToPojo = new ProtoPlanConverter(customExtensionCollection);
    io.substrait.plan.Plan pojoPlan = protoToPojo.from(originalProtoPlan);

    PlanProtoConverter pojoToProto = new PlanProtoConverter(customExtensionCollection);
    Plan finalProtoPlan = pojoToProto.toProto(pojoPlan);

    // 3. LOAD EXPECTED PROTO PLAN FROM JSON (should consolidate and have exhaustive coverage)
    Plan expectedProtoPlan =
        loadPlanFromJson("uri-urn-migration/mixed-partial-coverage-expected-plan.json");

    // 4. SIMPLE EQUALITY CHECK - All arithmetic functions should now have both URI and URN
    // references
    assertEquals(expectedProtoPlan, finalProtoPlan);
  }

  @Test
  public void testEmptyPlanMigration() throws IOException {
    // Test edge case: plan with no extensions at all
    BidiMap<String, String> uriUrnMap = new BidiMap<>();
    uriUrnMap.put(
        "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml",
        "extension:io.substrait:functions_arithmetic");

    // Create minimal plan with no extensions
    Plan originalProtoPlan =
        Plan.newBuilder()
            .setVersion(io.substrait.proto.Version.newBuilder().setMinorNumber(75))
            .addRelations(
                io.substrait.proto.PlanRel.newBuilder()
                    .setRoot(
                        io.substrait.proto.RelRoot.newBuilder()
                            .setInput(
                                io.substrait.proto.Rel.newBuilder()
                                    .setRead(
                                        io.substrait.proto.ReadRel.newBuilder()
                                            .setBaseSchema(
                                                io.substrait.proto.NamedStruct.newBuilder()
                                                    .addNames("dummy")
                                                    .setStruct(
                                                        io.substrait.proto.Type.Struct.newBuilder()
                                                            .addTypes(
                                                                io.substrait.proto.Type.newBuilder()
                                                                    .setI32(
                                                                        io.substrait.proto.Type.I32
                                                                            .newBuilder()))))
                                            .setNamedTable(
                                                io.substrait.proto.ReadRel.NamedTable.newBuilder()
                                                    .addNames("empty_table"))))
                            .addNames("dummy")))
            .build();

    // ROUNDTRIP: PROTO -> POJO -> PROTO
    SimpleExtension.ExtensionCollection customExtensionCollection =
        SimpleExtension.ExtensionCollection.builder()
            .from(defaultExtensions)
            .uriUrnMap(uriUrnMap)
            .build();
    ProtoPlanConverter protoToPojo = new ProtoPlanConverter(customExtensionCollection);
    io.substrait.plan.Plan pojoPlan = protoToPojo.from(originalProtoPlan);

    PlanProtoConverter pojoToProto = new PlanProtoConverter(customExtensionCollection);
    Plan finalProtoPlan = pojoToProto.toProto(pojoPlan);

    // Should remain empty but with proper nullability annotations
    assertEquals(0, finalProtoPlan.getExtensionUrisCount());
    assertEquals(0, finalProtoPlan.getExtensionUrnsCount());
    assertEquals(0, finalProtoPlan.getExtensionsCount());
  }
}
