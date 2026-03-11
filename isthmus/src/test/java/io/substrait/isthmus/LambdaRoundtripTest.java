package io.substrait.isthmus;

import com.google.protobuf.util.JsonFormat;
import io.substrait.plan.Plan;
import io.substrait.plan.ProtoPlanConverter;
import java.io.IOException;
import org.junit.jupiter.api.Test;

public class LambdaRoundtripTest extends PlanTestBase {

  public static io.substrait.proto.Plan readJsonPlan(String resourcePath) throws IOException {
    String json = asString(resourcePath);
    io.substrait.proto.Plan.Builder builder = io.substrait.proto.Plan.newBuilder();
    JsonFormat.parser().merge(json, builder);
    return builder.build();
  }

  @Test
  void testBasicLambdaRoundtrip() throws IOException {
    io.substrait.proto.Plan proto = readJsonPlan("lambdas/basic-lambda.json");
    Plan plan = new ProtoPlanConverter(extensions).from(proto);
    assertFullRoundTrip(plan.getRoots().get(0));
  }

  @Test
  void testLambdaWithFieldRefRoundtrip() throws IOException {
    io.substrait.proto.Plan proto = readJsonPlan("lambdas/lambda-field-ref.json");
    Plan plan = new ProtoPlanConverter(extensions).from(proto);
    assertFullRoundTrip(plan.getRoots().get(0));
  }

  @Test
  void testLambdaWithFunctionRoundtrip() throws IOException {
    io.substrait.proto.Plan proto = readJsonPlan("lambdas/lambda-with-function.json");
    Plan plan = new ProtoPlanConverter(extensions).from(proto);
    assertFullRoundTrip(plan.getRoots().get(0));
  }
}
