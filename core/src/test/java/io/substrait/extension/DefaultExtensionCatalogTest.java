package io.substrait.extension;

import static io.substrait.extension.DefaultExtensionCatalog.DEFAULT_COLLECTION;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
  void allExtensionYamlFilesAccountedFor() throws IOException, URISyntaxException {
    List<String> yamlFiles = getExtensionYamlFiles();

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
    try (InputStream is =
        DefaultExtensionCatalogTest.class.getClassLoader().getResourceAsStream(resourceName)) {
      assertNotNull(is, "Resource not found on classpath: " + resourceName);
      JsonNode doc = YAML_MAPPER.readTree(is);
      JsonNode urnNode = doc.get("urn");
      return urnNode == null ? null : urnNode.asText();
    }
  }

  /** Discovers extension YAML files on the classpath by locating a known resource. */
  private static List<String> getExtensionYamlFiles() throws URISyntaxException, IOException {
    var knownResource =
        DefaultExtensionCatalogTest.class.getClassLoader().getResource("functions_boolean.yaml");
    if (knownResource == null) {
      fail("Could not locate functions_boolean.yaml on classpath");
    }
    Path resourceDir = Paths.get(knownResource.toURI()).getParent();
    try (Stream<Path> files = Files.list(resourceDir)) {
      return files
          .filter(p -> p.toString().endsWith(".yaml"))
          .map(p -> p.getFileName().toString())
          .sorted()
          .collect(Collectors.toList());
    }
  }
}
