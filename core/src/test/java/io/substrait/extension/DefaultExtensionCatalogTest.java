package io.substrait.extension;

import static io.substrait.extension.DefaultExtensionCatalog.DEFAULT_COLLECTION;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Verifies that every extension YAML in substrait/extensions is loaded by {@link
 * DefaultExtensionCatalog}.
 */
class DefaultExtensionCatalogTest {

  private static final Set<String> UNSUPPORTED_FILES =
      Set.of(
          // TODO: aggregate_decimal_output defines count and approx_count_distinct with
          //  decimal<38,0> return types instead of i64. When loaded alongside aggregate_generic,
          //  the same function key (e.g. count:any) maps to the same Calcite operator twice,
          //  which breaks the reverse lookup in FunctionConverter.getSqlOperatorFromSubstraitFunc.
          //  Fixing this requires either deduplicating the operator map or adding type-based
          //  disambiguation for aggregate functions.
          "functions_aggregate_decimal_output.yaml",
          "functions_geometry.yaml", // user-defined types not supported in Calcite type conversion
          "functions_list.yaml", // TODO(#688): remove once lambda types are supported
          "type_variations.yaml", // type variations not yet supported by extension loader
          "unknown.yaml" // unknown type extension not yet loaded
          );

  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

  @Test
  void allExtensionYamlFilesAreLoaded() throws IOException {
    List<File> yamlFiles = getExtensionYamlFiles();

    for (File file : yamlFiles) {
      if (UNSUPPORTED_FILES.contains(file.getName())) {
        continue;
      }
      String urn = parseUrn(file);
      assertTrue(
          DEFAULT_COLLECTION.containsUrn(urn),
          file.getName() + " not loaded by DefaultExtensionCatalog (urn: " + urn + ")");
    }
  }

  private static String parseUrn(File yamlFile) throws IOException {
    JsonNode doc = YAML_MAPPER.readTree(yamlFile);
    JsonNode urnNode = doc.get("urn");
    return urnNode == null ? null : urnNode.asText();
  }

  private static List<File> getExtensionYamlFiles() {
    File extensionsDir = new File("../substrait/extensions");
    assertTrue(extensionsDir.isDirectory(), "substrait/extensions directory not found");
    File[] files = extensionsDir.listFiles((dir, name) -> name.endsWith(".yaml"));
    assertTrue(files != null && files.length > 0, "No YAML files found");
    return List.of(files);
  }
}
