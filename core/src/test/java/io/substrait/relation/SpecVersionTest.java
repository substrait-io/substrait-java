package io.substrait.relation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.substrait.plan.Plan.Version;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class SpecVersionTest {
  @Test
  void testSubstraitVersionDefaultValues() {
    Version version = Version.DEFAULT_VERSION;

    assertNotNull(version.getMajor());
    assertNotNull(version.getMinor());
    assertNotNull(version.getPatch());

    assertEquals(Optional.of("substrait-java"), version.getProducer());
  }
}
