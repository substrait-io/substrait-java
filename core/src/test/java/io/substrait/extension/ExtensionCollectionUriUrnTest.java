package io.substrait.extension;

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

    assertTrue(collection.hasUrn("file:///tmp/test.yaml"));
    assertTrue(collection.hasUri("extension:test:exists"));
    assertFalse(collection.hasUrn("nonexistent://uri"));
    assertFalse(collection.hasUri("extension:nonexistent:urn"));
  }

  @Test
  public void testGetNonexistentMappings() {
    String yamlContent =
        "%YAML 1.2\n" + "---\n" + "urn: extension:test:minimal\n" + "scalar_functions: []\n";

    SimpleExtension.ExtensionCollection collection =
        SimpleExtension.load("minimal://extension", yamlContent);

    assertNull(collection.getUrn("nonexistent://uri"));
    assertNull(collection.getUri("extension:nonexistent:urn"));
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

  @Test
  public void testMultipleExtensionsWithDifferentUris() {
    String yaml1 = "%YAML 1.2\n" + "---\n" + "urn: extension:ns1:ext1\n" + "scalar_functions: []\n";

    String yaml2 = "%YAML 1.2\n" + "---\n" + "urn: extension:ns2:ext2\n" + "scalar_functions: []\n";

    SimpleExtension.ExtensionCollection collection1 = SimpleExtension.load("uri1://test", yaml1);
    SimpleExtension.ExtensionCollection collection2 = SimpleExtension.load("uri2://test", yaml2);

    assertEquals("extension:ns1:ext1", collection1.getUrn("uri1://test"));
    assertEquals("extension:ns2:ext2", collection2.getUrn("uri2://test"));

    assertEquals("uri1://test", collection1.getUri("extension:ns1:ext1"));
    assertEquals("uri2://test", collection2.getUri("extension:ns2:ext2"));
  }
}
