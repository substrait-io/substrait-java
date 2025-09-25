package io.substrait.extension;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class ExtensionCollectionUriUrnTest {

  @Test
  public void testHasUrnAndHasUri() {
    String yamlContent =
        "%YAML 1.2\n"
            + "---\n"
            + "urn: extension:test:exists\n"
            + "scalar_functions:\n"
            + "  - name: test_function\n";

    SimpleExtension.ExtensionCollection collection =
        SimpleExtension.load("file:///tmp/test.yaml", yamlContent);

    assertTrue(collection.getUrnFromUri("file:///tmp/test.yaml") != null);
    assertTrue(collection.getUriFromUrn("extension:test:exists") != null);
    assertFalse(collection.getUrnFromUri("nonexistent://uri") != null);
    assertFalse(collection.getUriFromUrn("extension:nonexistent:urn") != null);
  }

  @Test
  public void testGetNonexistentMappings() {
    String yamlContent =
        "%YAML 1.2\n" + "---\n" + "urn: extension:test:minimal\n" + "scalar_functions: []\n";

    SimpleExtension.ExtensionCollection collection =
        SimpleExtension.load("minimal://extension", yamlContent);

    assertNull(collection.getUrnFromUri("nonexistent://uri"));
    assertNull(collection.getUriFromUrn("extension:nonexistent:urn"));
  }

  @Test
  public void testEmptyUriThrowsException() {
    String yamlContent =
        "%YAML 1.2\n" + "---\n" + "urn: extension:test:empty\n" + "scalar_functions: []\n";

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> SimpleExtension.load("", yamlContent));
    assertTrue(exception.getMessage().contains("URI cannot be null or empty"));
  }

  @Test
  public void testNullUriThrowsException() {
    String yamlContent =
        "%YAML 1.2\n" + "---\n" + "urn: extension:test:null\n" + "scalar_functions: []\n";

    // The system throws NPE when null is passed, which is expected behavior
    assertThrows(IllegalArgumentException.class, () -> SimpleExtension.load(null, yamlContent));
  }
}
