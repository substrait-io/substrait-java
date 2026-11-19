package io.substrait.isthmus;

import com.google.protobuf.util.JsonFormat;
import io.substrait.plan.Plan;
import io.substrait.plan.ProtoPlanConverter;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class LambdaRoundtripTest extends PlanTestBase {

  public static io.substrait.proto.Plan readJsonPlan(String resourcePath) throws IOException {
    String json = asString(resourcePath);
    io.substrait.proto.Plan.Builder builder = io.substrait.proto.Plan.newBuilder();
    JsonFormat.parser().merge(json, builder);
    return builder.build();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT transform(ARRAY[1, 2, 3], x -> x + 1)",
        "SELECT transform(ARRAY[1, 2, 3], x -> CAST(x AS BIGINT))",
        "SELECT transform(CAST(NULL AS INTEGER ARRAY), x -> x + 1)",
        "SELECT \"filter\"(ARRAY[1, 2, 3], x -> x > 1)",
        "SELECT any_match(ARRAY[1, 2, 3], x -> x = 2)",
        "SELECT all_match(ARRAY[1, 2, 3], x -> x > 0)"
      })
  void testSqlLambdaRoundtrip(String query) throws Exception {
    assertFullRoundTrip(query);
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
