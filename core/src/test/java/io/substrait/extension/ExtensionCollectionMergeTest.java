package io.substrait.extension;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ExtensionCollectionMergeTest {

  @Test
  void testMergeCollectionsWithDifferentUriUrnMappings() {
    String yaml1 =
        "%YAML 1.2\n"
            + "---\n"
            + "urn: extension:ns1:collection1\n"
            + "scalar_functions:\n"
            + "  - name: func1\n"
            + "    impls:\n"
            + "      - args: []\n"
            + "        return: boolean\n";

    String yaml2 =
        "%YAML 1.2\n"
            + "---\n"
            + "urn: extension:ns2:collection2\n"
            + "scalar_functions:\n"
            + "  - name: func2\n"
            + "    impls:\n"
            + "      - args: []\n"
            + "        return: i32\n";

    SimpleExtension.ExtensionCollection collection1 =
        SimpleExtension.load("uri1://extensions", yaml1);
    SimpleExtension.ExtensionCollection collection2 =
        SimpleExtension.load("uri2://extensions", yaml2);

    SimpleExtension.ExtensionCollection merged = collection1.merge(collection2);

    assertEquals("extension:ns1:collection1", merged.getUrnFromUri("uri1://extensions"));
    assertEquals("extension:ns2:collection2", merged.getUrnFromUri("uri2://extensions"));
    assertEquals("uri1://extensions", merged.getUriFromUrn("extension:ns1:collection1"));
    assertEquals("uri2://extensions", merged.getUriFromUrn("extension:ns2:collection2"));

    assertTrue(merged.scalarFunctions().size() >= 2);
  }

  @Test
  void testMergeCollectionsWithIdenticalMappings() {
    String yaml =
        "%YAML 1.2\n"
            + "---\n"
            + "urn: extension:shared:extension\n"
            + "scalar_functions:\n"
            + "  - name: shared_func\n"
            + "    impls:\n"
            + "      - args: []\n"
            + "        return: boolean\n";

    SimpleExtension.ExtensionCollection collection1 = SimpleExtension.load("shared://uri", yaml);
    SimpleExtension.ExtensionCollection collection2 = SimpleExtension.load("shared://uri", yaml);

    SimpleExtension.ExtensionCollection merged =
        assertDoesNotThrow(() -> collection1.merge(collection2));

    assertEquals("extension:shared:extension", merged.getUrnFromUri("shared://uri"));
    assertEquals("shared://uri", merged.getUriFromUrn("extension:shared:extension"));
  }

  @Test
  void testMergeCollectionsWithConflictingMappings() {
    String yaml1 =
        "%YAML 1.2\n" + "---\n" + "urn: extension:conflict:urn1\n" + "scalar_functions: []\n";

    String yaml2 =
        "%YAML 1.2\n" + "---\n" + "urn: extension:conflict:urn2\n" + "scalar_functions: []\n";

    SimpleExtension.ExtensionCollection collection1 = SimpleExtension.load("conflict://uri", yaml1);
    SimpleExtension.ExtensionCollection collection2 =
        SimpleExtension.load("conflict://uri", yaml2); // Same URI, different URN

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> collection1.merge(collection2));
    assertTrue(exception.getMessage().contains("Key already exists in map with different value"));
  }
}
