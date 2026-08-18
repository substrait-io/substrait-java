package io.substrait.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.substrait.TestBase;
import io.substrait.relation.Rel;
import io.substrait.relation.RelVisitor;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Guards the exhaustiveness the round trips built on {@link RelSamples} rely on. It lives here
 * rather than in one of them because both depend on it, so it must not disappear with either.
 */
class RelSamplesTest extends TestBase {

  @Test
  void everyRelationTypeHasASample() {
    Set<Class<?>> relTypes = new HashSet<>(RelSamples.relTypes());
    // Without this the comparison below passes vacuously if the reflection stops finding relations.
    assertFalse(relTypes.isEmpty(), "no relation types found on " + RelVisitor.class.getName());

    Set<Class<? extends Rel>> sampled =
        new HashSet<>(new RelSamples(sb, extensions).samples().keySet());

    assertEquals(relTypes, sampled, "every relation type needs a sample, and every sample a type");
  }
}
