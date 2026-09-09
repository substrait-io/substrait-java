package io.substrait.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.TestBase;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.junit.jupiter.api.Test;

/**
 * Verifies that type variations declared in the {@code type_variations} section of an extension
 * YAML file can be read and looked up by anchor, including their parent type class, description,
 * function behavior, and deprecation information.
 */
class TypeVariationExtensionTest extends TestBase {

  static final String URN = "extension:test:type_variation_extensions";
  static final SimpleExtension.ExtensionCollection TYPE_VARIATION_EXTENSION;

  static {
    try {
      String extensionStr = asString("extensions/type_variation_extensions.yaml");
      TYPE_VARIATION_EXTENSION = SimpleExtension.load(extensionStr);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  TypeVariationExtensionTest() {
    super(TYPE_VARIATION_EXTENSION);
  }

  @Test
  void parsesInheritsVariation() {
    SimpleExtension.TypeVariation variation =
        extensions.getTypeVariation(SimpleExtension.TypeVariationAnchor.of(URN, "dict4"));
    assertEquals("dict4", variation.name());
    assertEquals("string", variation.parent());
    assertEquals("a four-byte dictionary encoded string", variation.description().orElseThrow());
    assertEquals(SimpleExtension.TypeVariationFunctionBehavior.INHERITS, variation.functions());
    assertTrue(variation.deprecated().isEmpty());
  }

  @Test
  void parsesSeparateVariation() {
    SimpleExtension.TypeVariation variation =
        extensions.getTypeVariation(SimpleExtension.TypeVariationAnchor.of(URN, "avro"));
    assertEquals("struct", variation.parent());
    assertEquals(SimpleExtension.TypeVariationFunctionBehavior.SEPARATE, variation.functions());
  }

  @Test
  void functionBehaviorDefaultsToInherits() {
    SimpleExtension.TypeVariation variation =
        extensions.getTypeVariation(
            SimpleExtension.TypeVariationAnchor.of(URN, "inheritsByDefault"));
    assertEquals(SimpleExtension.TypeVariationFunctionBehavior.INHERITS, variation.functions());
  }

  @Test
  void parsesDeprecation() {
    SimpleExtension.DeprecationStatus deprecation =
        extensions
            .getTypeVariation(SimpleExtension.TypeVariationAnchor.of(URN, "deprecatedVariation"))
            .deprecated()
            .orElseThrow();
    assertEquals("0.86.0", deprecation.since());
    assertEquals("Replaced by avro", deprecation.reason().orElseThrow());
  }

  @Test
  void unknownVariationThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () -> extensions.getTypeVariation(SimpleExtension.TypeVariationAnchor.of(URN, "missing")));
  }
}
