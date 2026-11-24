package io.substrait.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Verifies the behavior and failure modes of the {@link SimpleExtension} {@code load} overloads.
 */
class SimpleExtensionLoadTest {

  private static final String MISSING_RESOURCE = "/substrait/extensions/does_not_exist.yaml";
  private static final String PRESENT_RESOURCE = "/substrait/extensions/functions_boolean.yaml";
  private static final String SECOND_RESOURCE = "/substrait/extensions/functions_set.yaml";
  private static final String UNSIGNED_RESOURCE = "/substrait/extensions/unsigned_integers.yaml";

  @Test
  void loadEmptyResourcePathsThrows() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> SimpleExtension.load(Collections.emptyList()));
    assertTrue(e.getMessage().contains("Require at least one resource path"), e.getMessage());
  }

  @Test
  void loadMissingResourceNamesThePath() {
    List<String> paths = Collections.singletonList(MISSING_RESOURCE);
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> SimpleExtension.load(paths));
    assertTrue(e.getMessage().contains(MISSING_RESOURCE), e.getMessage());
  }

  @Test
  void loadMissingResourceAmongPresentOnesNamesTheMissingPath() {
    List<String> paths = Arrays.asList(PRESENT_RESOURCE, MISSING_RESOURCE);
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> SimpleExtension.load(paths));
    assertTrue(e.getMessage().contains(MISSING_RESOURCE), e.getMessage());
  }

  @Test
  void loadNullStreamThrowsWithAMessage() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> SimpleExtension.load((InputStream) null));
    assertNotNull(e.getMessage());
  }

  @Test
  void loadPresentResourceSucceeds() {
    SimpleExtension.ExtensionCollection collection =
        SimpleExtension.load(Collections.singletonList(PRESENT_RESOURCE));
    assertTrue(collection.scalarFunctions().size() > 0);
  }

  @Test
  void loadWithCommentsAndDirectivesBeforeUrn() {
    String yaml =
        "%YAML 1.2\n"
            + "# leading comment\n"
            + "---\n"
            + "# comment before urn\n"
            + "urn: extension:test:comments\n"
            + "scalar_functions:\n"
            + "  - name: foo\n"
            + "    impls:\n"
            + "      - args:\n"
            + "          - value: i32\n"
            + "        return: i32\n";
    SimpleExtension.ExtensionCollection collection = SimpleExtension.load(yaml);
    SimpleExtension.ScalarFunctionVariant fn =
        collection.getScalarFunction(
            SimpleExtension.FunctionAnchor.of("extension:test:comments", "foo:i32"));
    assertEquals("extension:test:comments", fn.urn());
  }

  @Test
  void loadWithUrnAfterNestedSectionsAndNestedUrnKey() {
    String yaml =
        "%YAML 1.2\n"
            + "---\n"
            + "metadata:\n"
            + "  urn: extension:nested:should_be_ignored\n"
            + "  tags:\n"
            + "    - alpha\n"
            + "    - beta\n"
            + "types:\n"
            + "  - name: custom_t\n"
            + "scalar_functions:\n"
            + "  - name: bar\n"
            + "    impls:\n"
            + "      - args:\n"
            + "          - value: u!custom_t\n"
            + "        return: u!custom_t\n"
            + "urn: extension:test:late_urn\n";
    SimpleExtension.ExtensionCollection collection = SimpleExtension.load(yaml);
    SimpleExtension.Type type =
        collection.getType(SimpleExtension.TypeAnchor.of("extension:test:late_urn", "custom_t"));
    assertEquals("extension:test:late_urn", type.urn());
    SimpleExtension.ScalarFunctionVariant fn =
        collection.getScalarFunction(
            SimpleExtension.FunctionAnchor.of("extension:test:late_urn", "bar:u!custom_t"));
    assertEquals("extension:test:late_urn", fn.urn());
  }

  @Test
  void loadWithNullOrNonStringUrnThrowsIllegalArgument() {
    List<String> invalidYamls =
        Arrays.asList(
            "urn: null\n",
            "urn:\n",
            "urn: 123\n",
            "urn: true\n",
            "urn:\n  - extension:test:list\n",
            "urn:\n  nested: extension:test:obj\n");
    for (String yaml : invalidYamls) {
      assertThrows(
          IllegalArgumentException.class,
          () -> SimpleExtension.load(yaml),
          "Expected IllegalArgumentException for YAML: " + yaml);
    }
  }

  @Test
  void loadWithEmptyOrNonObjectYamlThrowsIllegalArgument() {
    List<String> nonObjectYamls =
        Arrays.asList("", "   \n# only comment\n", "- item1\n- item2\n", "just_a_scalar\n");
    for (String yaml : nonObjectYamls) {
      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class,
              () -> SimpleExtension.load(yaml),
              "Expected IllegalArgumentException for YAML: " + yaml);
      assertTrue(
          e.getMessage().contains("Extension YAML file must contain a 'urn' field"),
          e.getMessage());
    }
  }

  @Test
  void loadMalformedYamlThrowsIllegalState() {
    String malformedBeforeUrn = "key: [unclosed\nurn: extension:test:bad\n";
    assertThrows(IllegalStateException.class, () -> SimpleExtension.load(malformedBeforeUrn));

    String malformedAfterUrn = "urn: extension:test:bad\nscalar_functions: [unclosed\n";
    assertThrows(IllegalStateException.class, () -> SimpleExtension.load(malformedAfterUrn));
  }

  @Test
  void loadFromInputStreamHandlesUtf8AndMultiChunkContent() {
    StringBuilder description = new StringBuilder("UTF-8 café ");
    for (int i = 0; i < 1000; i++) {
      description.append("0123456789");
    }
    String yaml =
        "%YAML 1.2\n"
            + "---\n"
            + "urn: extension:test:stream\n"
            + "scalar_functions:\n"
            + "  - name: f\n"
            + "    description: \""
            + description
            + "\"\n"
            + "    impls:\n"
            + "      - args:\n"
            + "          - value: i64\n"
            + "        return: i64\n";
    InputStream stream = new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8));
    SimpleExtension.ExtensionCollection collection = SimpleExtension.load(stream);
    SimpleExtension.ScalarFunctionVariant fn =
        collection.getScalarFunction(
            SimpleExtension.FunctionAnchor.of("extension:test:stream", "f:i64"));
    assertEquals(description.toString(), fn.description());
  }

  @Test
  void loadMultipleResourcesMatchesSequentialMerge() {
    List<String> resources = Arrays.asList(PRESENT_RESOURCE, SECOND_RESOURCE, UNSIGNED_RESOURCE);
    SimpleExtension.ExtensionCollection multi = SimpleExtension.load(resources);
    SimpleExtension.ExtensionCollection merged =
        SimpleExtension.load(Collections.singletonList(PRESENT_RESOURCE))
            .merge(SimpleExtension.load(Collections.singletonList(SECOND_RESOURCE)))
            .merge(SimpleExtension.load(Collections.singletonList(UNSIGNED_RESOURCE)));

    assertEquals(merged.scalarFunctions(), multi.scalarFunctions());
    assertEquals(merged.aggregateFunctions(), multi.aggregateFunctions());
    assertEquals(merged.windowFunctions(), multi.windowFunctions());
    assertEquals(merged.types(), multi.types());
    assertEquals(merged.extensionMetadata(), multi.extensionMetadata());
  }
}
