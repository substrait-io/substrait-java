package io.substrait.extension;

import static io.substrait.extension.DefaultExtensionCatalog.DEFAULT_COLLECTION;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ResourceList;
import io.github.classgraph.ScanResult;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Verifies that every extension YAML in substrait/extensions is loaded by {@link
 * DefaultExtensionCatalog}.
 */
class DefaultExtensionCatalogTest {

  /**
   * Extension YAML files that are intentionally not loaded by {@link DefaultExtensionCatalog}.
   * Adding a file here requires a comment explaining why it cannot be loaded.
   */
  private static final Set<String> UNSUPPORTED_FILES =
      Set.of(
          // aggregate_decimal_output defines count and approx_count_distinct with decimal<38,0>
          // return types instead of i64. When loaded alongside aggregate_generic, the same
          // function key (e.g. count:any) maps to the same Calcite operator twice, which breaks
          // the reverse lookup in FunctionConverter.getSqlOperatorFromSubstraitFunc.
          "functions_aggregate_decimal_output.yaml",
          // functions_geometry.yaml defines user-defined types (u!geometry) that are not
          // supported by Calcite's type conversion in isthmus.
          "functions_geometry.yaml",
          // type_variations.yaml only defines type variations, which are not tracked by
          // ExtensionCollection (no functions or types), so containsUrn cannot verify it.
          "type_variations.yaml",
          // unknown.yaml uses an "unknown" type that is not a recognized type literal or
          // parameterized type, causing Function.constructKey to fail at load time.
          "unknown.yaml");

  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

  @Test
  void defaultCollectionLoads() {
    assertNotNull(DEFAULT_COLLECTION);
  }

  @Test
  void allExtensionYamlFilesAccountedFor() throws IOException {
    List<String> yamlFiles = getExtensionYamlFiles();
    assertFalse(yamlFiles.isEmpty(), "No YAML files found in substrait/extensions/");

    for (String fileName : yamlFiles) {
      String urn = parseUrn(fileName);
      assertNotNull(urn, fileName + " does not contain a URN field");
      if (UNSUPPORTED_FILES.contains(fileName)) {
        assertFalse(
            DEFAULT_COLLECTION.containsUrn(urn),
            fileName
                + " is in UNSUPPORTED_FILES but is loaded by DefaultExtensionCatalog"
                + " — remove it from UNSUPPORTED_FILES");
      } else {
        assertTrue(
            DEFAULT_COLLECTION.containsUrn(urn),
            fileName + " not loaded by DefaultExtensionCatalog (urn: " + urn + ")");
      }
    }
  }

  private static String parseUrn(String resourceName) throws IOException {
    String resourcePath = "substrait/extensions/" + resourceName;
    try (InputStream is =
        DefaultExtensionCatalogTest.class.getClassLoader().getResourceAsStream(resourcePath)) {
      assertNotNull(is, "Resource not found on classpath: " + resourcePath);
      JsonNode doc = YAML_MAPPER.readTree(is);
      JsonNode urnNode = doc.get("urn");
      return urnNode == null ? null : urnNode.asText();
    }
  }

  private static List<String> getExtensionYamlFiles() {
    try (ScanResult scan =
        new ClassGraph().acceptPathsNonRecursive("substrait/extensions").scan()) {
      ResourceList resources = scan.getResourcesWithExtension(".yaml");
      return resources.stream()
          .map(r -> r.getPath().substring("substrait/extensions/".length()))
          .sorted()
          .toList();
    }
  }
}
