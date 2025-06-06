package io.substrait.relation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.substrait.plan.ImmutableVersion;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class SpecVersionTest {
  @Test
  public void testSubstraitVersionDefaultValues() {
    ImmutableVersion version = ImmutableVersion.builder().build();

    assertNotNull(version.getMajor());
    assertNotNull(version.getMinor());
    assertNotNull(version.getPatch());

    assertEquals(Optional.of("substrait-java"), version.getProducer());
  }
}
