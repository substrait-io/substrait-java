package io.substrait.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.TestBase;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Verifies that metadata can be read from extension YAML files at multiple levels:
 *
 * <ul>
 *   <li>Extension-level metadata (top-level)
 *   <li>Type-level metadata
 *   <li>Function-level metadata (scalar, aggregate, window)
 * </ul>
 */
class MetadataExtensionTest extends TestBase {

  static final String URN = "extension:test:metadata_extensions";
  static final SimpleExtension.ExtensionCollection METADATA_EXTENSION;

  static {
    try {
      String extensionStr = asString("extensions/metadata_extensions.yaml");
      METADATA_EXTENSION = SimpleExtension.load(URN, extensionStr);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  MetadataExtensionTest() {
    super(METADATA_EXTENSION);
  }

  @Test
  void testExtensionLevelMetadata() {
    Map<String, Object> metadata = extensions.getExtensionMetadata(URN).orElseThrow();
    assertEquals("1.0", metadata.get("version"));
    assertEquals("test-team", metadata.get("author"));

    @SuppressWarnings("unchecked")
    Map<String, Object> customData = (Map<String, Object>) metadata.get("custom_data");
    assertEquals(true, customData.get("nested_value"));
    assertEquals(42, customData.get("numeric_value"));
  }

  @Test
  void testExtensionLevelMetadataMissing() {
    assertTrue(extensions.getExtensionMetadata("extension:nonexistent:urn").isEmpty());
  }

  @Test
  void testTypeMetadata() {
    SimpleExtension.TypeAnchor anchor = SimpleExtension.TypeAnchor.of(URN, "metadataType");
    Map<String, Object> metadata = extensions.getType(anchor).metadata().orElseThrow();
    assertEquals("custom-type-metadata", metadata.get("type_info"));
    assertEquals("user-defined", metadata.get("category"));
  }

  @Test
  void testScalarFunctionMetadata() {
    SimpleExtension.FunctionAnchor anchor =
        SimpleExtension.FunctionAnchor.of(URN, "metadataScalar:i64");
    Map<String, Object> metadata = extensions.getScalarFunction(anchor).metadata().orElseThrow();
    assertEquals("vectorized", metadata.get("perf_hint"));
    assertEquals(1, metadata.get("cost"));
  }

  @Test
  void testAggregateFunctionMetadata() {
    SimpleExtension.FunctionAnchor anchor =
        SimpleExtension.FunctionAnchor.of(URN, "metadataAggregate:i64");
    assertEquals(
        "incremental",
        extensions.getAggregateFunction(anchor).metadata().orElseThrow().get("agg_info"));
  }

  @Test
  void testWindowFunctionMetadata() {
    SimpleExtension.FunctionAnchor anchor =
        SimpleExtension.FunctionAnchor.of(URN, "metadataWindow:i64");
    assertEquals(
        "partitioned",
        extensions.getWindowFunction(anchor).metadata().orElseThrow().get("window_info"));
  }

  @Test
  void testMergePreservesMetadata() throws IOException {
    String customExtensionStr = asString("extensions/custom_extensions.yaml");
    SimpleExtension.ExtensionCollection customExtension =
        SimpleExtension.load("extension:test:custom_extensions", customExtensionStr);

    SimpleExtension.ExtensionCollection merged = METADATA_EXTENSION.merge(customExtension);

    assertEquals("1.0", merged.getExtensionMetadata(URN).orElseThrow().get("version"));
    assertTrue(merged.getExtensionMetadata("extension:test:custom_extensions").isEmpty());
  }
}
