package io.substrait.docs;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.substrait.TestBase;
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.relation.Rel;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Backs the code sample in {@code docs/getting-started.md}. The region is pulled into the docs via
 * a {@code --8<--} snippet include.
 */
class GettingStartedDocTest extends TestBase {

  @Test
  void quickStart() {
    // --8<-- [start:quick-start]
    SubstraitBuilder builder = new SubstraitBuilder();
    TypeCreator R = TypeCreator.REQUIRED;

    // SELECT id, customer FROM orders WHERE id = 1
    Rel scan =
        builder.namedScan(List.of("orders"), List.of("id", "customer"), List.of(R.I32, R.STRING));

    Rel filtered =
        builder.filter(
            input -> builder.equal(builder.fieldReference(input, 0), builder.i32(1)), scan);

    Plan.Root root = builder.root(filtered, List.of("id", "customer"));
    Plan plan = builder.plan(root);

    // Convert the POJO plan to the Substrait protobuf message
    io.substrait.proto.Plan proto = new PlanProtoConverter().toProto(plan);
    byte[] bytes = proto.toByteArray();
    // --8<-- [end:quick-start]
    assertNotNull(bytes);
  }
}
