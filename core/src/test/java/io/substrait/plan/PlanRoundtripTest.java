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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Roundtrip tests: parse a JSON proto plan, convert to POJO, convert back to proto, and compare
 * with the expected output.
 */
class PlanRoundtripTest {

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

  static Stream<Arguments> roundtripCases() {
    return Stream.of(
        Arguments.of(
            "simple",
            "plan-roundtrip/simple-input-plan.json",
            "plan-roundtrip/simple-expected-plan.json"),
        Arguments.of(
            "complex",
            "plan-roundtrip/complex-input-plan.json",
            "plan-roundtrip/complex-expected-plan.json"),
        Arguments.of(
            "zero-anchor",
            "plan-roundtrip/zero-anchor-input-plan.json",
            "plan-roundtrip/zero-anchor-expected-plan.json"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("roundtripCases")
  void testPlanRoundtrip(String name, String inputPath, String expectedPath) throws IOException {
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
