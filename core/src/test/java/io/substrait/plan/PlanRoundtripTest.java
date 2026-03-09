package io.substrait.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.util.JsonFormat;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.proto.Plan;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Roundtrip tests: parse a JSON proto plan, convert to POJO, convert back to proto, and compare
 * with the expected output.
 */
class PlanRoundtripTest {

  private static final List<String[]> TEST_CASES =
      List.of(
          new String[] {
            "plan-roundtrip/simple-input-plan.json", "plan-roundtrip/simple-expected-plan.json"
          },
          new String[] {
            "plan-roundtrip/complex-input-plan.json", "plan-roundtrip/complex-expected-plan.json"
          },
          new String[] {
            "plan-roundtrip/zero-anchor-input-plan.json",
            "plan-roundtrip/zero-anchor-expected-plan.json"
          });

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

  private void testPlanRoundtrip(Plan inputPlan, Plan expectedPlan) {
    ProtoPlanConverter protoToPojo =
        new ProtoPlanConverter(DefaultExtensionCatalog.DEFAULT_COLLECTION);
    io.substrait.plan.Plan pojoPlan = protoToPojo.from(inputPlan);

    PlanProtoConverter pojoToProto =
        new PlanProtoConverter(DefaultExtensionCatalog.DEFAULT_COLLECTION);
    Plan actualPlan = pojoToProto.toProto(pojoPlan);

    assertEquals(expectedPlan, actualPlan);
  }

  @Test
  void testAllPlanRoundtrips() throws IOException {
    for (String[] testCase : TEST_CASES) {
      Plan inputPlan = loadPlanFromJson(testCase[0]);
      Plan expectedPlan = loadPlanFromJson(testCase[1]);
      testPlanRoundtrip(inputPlan, expectedPlan);
    }
  }
}
