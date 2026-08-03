package io.substrait.isthmus.docs;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.substrait.isthmus.PlanTestBase;
import io.substrait.isthmus.SqlToSubstrait;
import org.junit.jupiter.api.Test;

/**
 * Backs the code sample in {@code docs/isthmus/index.md}. The region is pulled into the docs via a
 * {@code --8<--} snippet include.
 */
class IsthmusIndexDocTest extends PlanTestBase {

  @Test
  void entryPoint() {
    // --8<-- [start:entry-point]
    // Uses ConverterProvider defaults
    SqlToSubstrait converter = new SqlToSubstrait();
    // --8<-- [end:entry-point]
    assertNotNull(converter);
  }
}
