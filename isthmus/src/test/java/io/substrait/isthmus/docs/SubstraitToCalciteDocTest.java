package io.substrait.isthmus.docs;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.substrait.isthmus.ConverterProvider;
import io.substrait.isthmus.PlanTestBase;
import io.substrait.isthmus.SubstraitToCalcite;
import io.substrait.plan.Plan.Root;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.apache.calcite.rel.RelRoot;
import org.junit.jupiter.api.Test;

/**
 * Backs the code sample in {@code docs/isthmus/substrait-to-calcite.md}. The region is pulled into
 * the docs via a {@code --8<--} snippet include.
 */
class SubstraitToCalciteDocTest extends PlanTestBase {

  @Test
  void example() {
    // --8<-- [start:example]
    Iterable<Type> types = List.of(TypeCreator.REQUIRED.I64, TypeCreator.REQUIRED.STRING);
    Root root =
        Root.builder()
            .input(sb.namedScan(List.of("stores"), List.of("s_store_id", "s"), types))
            .addNames("s_store_id", "store")
            .build();

    SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(new ConverterProvider());
    RelRoot relRoot = substraitToCalcite.convert(root);

    // relRoot.fields carries the top-level output names: [s_store_id, store]
    // --8<-- [end:example]
    assertNotNull(relRoot);
  }
}
