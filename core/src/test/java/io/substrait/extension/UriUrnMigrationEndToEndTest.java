package io.substrait.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.util.JsonFormat;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.proto.Plan;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests demonstrating the full URI/URN migration workflow: 1. Consume plans with mixed
 * URI/URN references 2. Convert proto -> POJO using ImmutableExtensionLookup with URI/URN mapping
 * 3. Convert POJO -> proto using PlanProtoConverter 4. Verify output contains proper
 * extensioninformation
 */
public class UriUrnMigrationEndToEndTest {

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
  public void testUriUrnMigrationEndToEnd() throws IOException {

    // List of (inputPath, expectedPath, extensionCollection) tuples
    List<String[]> testCases =
        Arrays.asList(
            new String[] {
              "uri-urn-migration/uri-only-input-plan.json",
              "uri-urn-migration/uri-only-expected-plan.json"
            },
            new String[] {
              "uri-urn-migration/complex-input-plan.json",
              "uri-urn-migration/complex-expected-plan.json"
            },
            new String[] {
              "uri-urn-migration/urn-only-input-plan.json",
              "uri-urn-migration/urn-only-expected-plan.json"
            },
            new String[] {
              "uri-urn-migration/mixed-partial-coverage-input-plan.json",
              "uri-urn-migration/mixed-partial-coverage-expected-plan.json"
            },
            new String[] {
              "uri-urn-migration/zero-urn-resolution-input-plan.json",
              "uri-urn-migration/zero-urn-resolution-expected-plan.json"
            });

    for (String[] testCase : testCases) {
      String inputPath = testCase[0];
      String expectedPath = testCase[1];

      Plan inputPlan = loadPlanFromJson(inputPath);
      Plan expectedPlan = loadPlanFromJson(expectedPath);

      ProtoPlanConverter protoToPojo =
          new ProtoPlanConverter(DefaultExtensionCatalog.DEFAULT_COLLECTION);
      io.substrait.plan.Plan pojoPlan = protoToPojo.from(inputPlan);

      PlanProtoConverter pojoToProto =
          new PlanProtoConverter(DefaultExtensionCatalog.DEFAULT_COLLECTION);
      Plan actualPlan = pojoToProto.toProto(pojoPlan);

      assertEquals(expectedPlan, actualPlan);
    }
  }

  @Test
  public void testUnresolvableUriThrowsException() throws IOException {
    Plan inputPlan = loadPlanFromJson("uri-urn-migration/unresolvable-uri-plan.json");

    ProtoPlanConverter protoToPojo =
        new ProtoPlanConverter(DefaultExtensionCatalog.DEFAULT_COLLECTION);

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> {
              protoToPojo.from(inputPlan);
            });

    assertTrue(exception.getMessage().contains("All resolution strategies failed"));
    assertTrue(exception.getMessage().contains("/functions_nonexistent.yaml"));
  }
}
