package io.substrait.relation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.TestBase;
import io.substrait.extension.AdvancedExtension;
import io.substrait.hint.Hint;
import io.substrait.utils.RelSamples;
import io.substrait.utils.StringHolder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link HasExtension#withExtension(Optional)}, the type-agnostic copy method for the
 * extension a value carries directly.
 *
 * <p>It is a throwing default that the generated {@code ImmutableXxx} overrides on signature match
 * alone — no {@code @Override} links the two. Narrowing the declared parameter type clashes with
 * what Immutables emits and so fails the build, but a change in the shape of the generated copy
 * method would not: the default would simply start being reached at runtime. That is what these
 * assertions catch.
 */
class HasExtensionTest extends TestBase {

  final AdvancedExtension extension =
      AdvancedExtension.builder()
          .enhancement(new StringHolder("ENHANCEMENT"))
          .addOptimizations(new StringHolder("OPTIMIZATION"))
          .build();

  final Map<Class<? extends Rel>, Rel> relSamples =
      new RelSamples(sb, extensions)
          .samples().entrySet().stream()
              .filter(sample -> sample.getValue() instanceof HasExtension)
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

  @Test
  void generationOverridesTheDefaultForEveryRelation() {
    // Exhaustive over the shared samples, which RelSamplesTest holds to every relation the model
    // has, so a relation whose generated withExtension does not match cannot slip through.
    assertFalse(relSamples.isEmpty(), "No HasExtension relation samples found");

    for (Map.Entry<Class<? extends Rel>, Rel> sample : relSamples.entrySet()) {
      Class<? extends Rel> relType = sample.getKey();
      HasExtension rel = (HasExtension) sample.getValue();

      HasExtension extended = rel.withExtension(Optional.of(extension));

      assertEquals(
          Optional.of(extension),
          extended.getExtension(),
          relType.getSimpleName() + " did not carry the extension it was given");
      // The copy stays the same relation type, which is what lets a caller holding a Rel cast the
      // result back to one.
      assertTrue(
          relType.isInstance(extended),
          relType.getSimpleName() + " returned " + extended.getClass().getName());
      // Clearing it again gets back to the sample, so the copy changed nothing else.
      assertEquals(
          sample.getValue(),
          extended.withExtension(Optional.empty()),
          relType.getSimpleName() + " did not clear the extension");
    }
  }

  @Test
  void nonRelationImplementorsCarryItToo() {
    // Hint and its nested types implement HasExtension without being relations, which is why
    // withExtension returns HasExtension rather than Rel.
    List<HasExtension> values =
        List.of(
            Hint.builder().build(),
            Hint.Stats.builder().rowCount(1).recordSize(1).build(),
            Hint.RuntimeConstraint.builder().build());

    for (HasExtension value : values) {
      HasExtension extended = value.withExtension(Optional.of(extension));

      assertEquals(Optional.of(extension), extended.getExtension());
      assertEquals(value, extended.withExtension(Optional.empty()));
    }
  }

  @Test
  void handWrittenImplementorInheritsTheThrowingDefault() {
    // A HasExtension that is not Immutables-backed: getExtension is the only abstract method.
    HasExtension custom = Optional::empty;

    UnsupportedOperationException e =
        assertThrows(
            UnsupportedOperationException.class,
            () -> custom.withExtension(Optional.of(extension)));
    assertTrue(
        e.getMessage().contains("does not support setting an extension"),
        "Unexpected message: " + e.getMessage());
  }
}
