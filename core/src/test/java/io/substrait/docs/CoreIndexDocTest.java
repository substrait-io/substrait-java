package io.substrait.docs;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.substrait.TestBase;
import io.substrait.plan.Plan;
import io.substrait.relation.NamedScan;
import io.substrait.type.NamedStruct;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Backs the code sample in {@code docs/core/index.md}. The region is pulled into the docs via a
 * {@code --8<--} snippet include.
 */
class CoreIndexDocTest extends TestBase {

  @Test
  void quickExample() {
    NamedStruct schema = NamedStruct.of(List.of("id"), R.struct(R.I32));
    // --8<-- [start:quick-example]
    NamedScan scan = NamedScan.builder().addNames("my_table").initialSchema(schema).build();

    Plan.Root root = Plan.Root.builder().input(scan).build();
    Plan plan =
        Plan.builder()
            .addRoots(root)
            .executionBehavior(
                Plan.ExecutionBehavior.builder()
                    .variableEvaluationMode(Plan.ExecutionBehavior.VariableEvaluationMode.PER_PLAN)
                    .build())
            .build();
    // --8<-- [end:quick-example]
    assertNotNull(plan);
  }
}
