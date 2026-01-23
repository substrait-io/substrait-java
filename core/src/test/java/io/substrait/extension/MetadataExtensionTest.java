package io.substrait.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.TestBase;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Optional;
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
    Optional<Map<String, Object>> metadata = extensions.getExtensionMetadata(URN);
    assertTrue(metadata.isPresent(), "Extension metadata should be present");

    Map<String, Object> meta = metadata.get();
    assertEquals("1.0", meta.get("version"));
    assertEquals("test-team", meta.get("author"));

    // Test nested metadata
    @SuppressWarnings("unchecked")
    Map<String, Object> customData = (Map<String, Object>) meta.get("custom_data");
    assertEquals(true, customData.get("nested_value"));
    assertEquals(42, customData.get("numeric_value"));
  }

  @Test
  void testExtensionLevelMetadataMissing() {
    Optional<Map<String, Object>> metadata =
        extensions.getExtensionMetadata("extension:nonexistent:urn");
    assertFalse(metadata.isPresent(), "Metadata for non-existent URN should be empty");
  }

  @Test
  void testTypeMetadata() {
    SimpleExtension.TypeAnchor anchor = SimpleExtension.TypeAnchor.of(URN, "metadataType");
    SimpleExtension.Type type = extensions.getType(anchor);

    Optional<Map<String, Object>> metadata = type.metadata();
    assertTrue(metadata.isPresent(), "Type metadata should be present");

    Map<String, Object> meta = metadata.get();
    assertEquals("custom-type-metadata", meta.get("type_info"));
    assertEquals("user-defined", meta.get("category"));
  }

  @Test
  void testScalarFunctionMetadata() {
    SimpleExtension.FunctionAnchor anchor =
        SimpleExtension.FunctionAnchor.of(URN, "metadataScalar:i64");
    SimpleExtension.ScalarFunctionVariant fn = extensions.getScalarFunction(anchor);

    Optional<Map<String, Object>> metadata = fn.metadata();
    assertTrue(metadata.isPresent(), "Scalar function metadata should be present");

    Map<String, Object> meta = metadata.get();
    assertEquals("vectorized", meta.get("perf_hint"));
    assertEquals(1, meta.get("cost"));
  }

  @Test
  void testAggregateFunctionMetadata() {
    SimpleExtension.FunctionAnchor anchor =
        SimpleExtension.FunctionAnchor.of(URN, "metadataAggregate:i64");
    SimpleExtension.AggregateFunctionVariant fn = extensions.getAggregateFunction(anchor);

    Optional<Map<String, Object>> metadata = fn.metadata();
    assertTrue(metadata.isPresent(), "Aggregate function metadata should be present");

    Map<String, Object> meta = metadata.get();
    assertEquals("incremental", meta.get("agg_info"));
  }

  @Test
  void testWindowFunctionMetadata() {
    SimpleExtension.FunctionAnchor anchor =
        SimpleExtension.FunctionAnchor.of(URN, "metadataWindow:i64");
    SimpleExtension.WindowFunctionVariant fn = extensions.getWindowFunction(anchor);

    Optional<Map<String, Object>> metadata = fn.metadata();
    assertTrue(metadata.isPresent(), "Window function metadata should be present");

    Map<String, Object> meta = metadata.get();
    assertEquals("partitioned", meta.get("window_info"));
  }

  @Test
  void testMergePreservesMetadata() throws IOException {
    // Load a second extension without metadata
    String customExtensionStr = asString("extensions/custom_extensions.yaml");
    SimpleExtension.ExtensionCollection customExtension =
        SimpleExtension.load("extension:test:custom_extensions", customExtensionStr);

    // Merge the two collections
    SimpleExtension.ExtensionCollection merged = METADATA_EXTENSION.merge(customExtension);

    // Verify metadata from first extension is still accessible
    Optional<Map<String, Object>> metadata = merged.getExtensionMetadata(URN);
    assertTrue(metadata.isPresent(), "Metadata should be preserved after merge");
    assertEquals("1.0", metadata.get().get("version"));

    // Verify the second extension has no metadata
    Optional<Map<String, Object>> customMetadata =
        merged.getExtensionMetadata("extension:test:custom_extensions");
    assertFalse(customMetadata.isPresent(), "Custom extension should have no metadata");
  }
}
