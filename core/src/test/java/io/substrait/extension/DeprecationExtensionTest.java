package io.substrait.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.TestBase;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Verifies that deprecation information can be read from extension YAML files at multiple levels:
 *
 * <ul>
 *   <li>Type-level deprecation
 *   <li>Function-level deprecation (scalar, aggregate, window)
 *   <li>Function-implementation-level deprecation (individual overloads)
 * </ul>
 *
 * See <a href="https://github.com/substrait-io/substrait/pull/1014">substrait#1014</a>.
 */
class DeprecationExtensionTest extends TestBase {

  static final String URN = "extension:test:deprecation_extensions";
  static final SimpleExtension.ExtensionCollection DEPRECATION_EXTENSION;

  static {
    try {
      String extensionStr = asString("extensions/deprecation_extensions.yaml");
      DEPRECATION_EXTENSION = SimpleExtension.load(extensionStr);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  DeprecationExtensionTest() {
    super(DEPRECATION_EXTENSION);
  }

  @Test
  void testTypeDeprecation() {
    SimpleExtension.TypeAnchor anchor = SimpleExtension.TypeAnchor.of(URN, "deprecatedType");
    SimpleExtension.DeprecationStatus deprecation =
        extensions.getType(anchor).deprecated().orElseThrow();
    assertEquals("0.50.0", deprecation.since());
    assertEquals("Replaced by newType", deprecation.reason().orElseThrow());
  }

  @Test
  void testScalarFunctionDeprecation() {
    SimpleExtension.FunctionAnchor anchor =
        SimpleExtension.FunctionAnchor.of(URN, "deprecatedScalar:i64");
    SimpleExtension.DeprecationStatus deprecation =
        extensions.getScalarFunction(anchor).deprecated().orElseThrow();
    assertEquals("0.1.1", deprecation.since());
    assertEquals("Use newScalar instead", deprecation.reason().orElseThrow());
  }

  @Test
  void testScalarFunctionDeprecationWithMetadata() {
    SimpleExtension.FunctionAnchor anchor =
        SimpleExtension.FunctionAnchor.of(URN, "deprecatedScalarWithMetadata:i64");
    SimpleExtension.DeprecationStatus deprecation =
        extensions.getScalarFunction(anchor).deprecated().orElseThrow();
    assertEquals("2.0.0", deprecation.since());
    Map<String, Object> metadata = deprecation.metadata().orElseThrow();
    assertEquals("newScalar", metadata.get("alternative"));
  }

  @Test
  void testAggregateFunctionDeprecation() {
    SimpleExtension.FunctionAnchor anchor =
        SimpleExtension.FunctionAnchor.of(URN, "deprecatedAggregate:i64");
    SimpleExtension.DeprecationStatus deprecation =
        extensions.getAggregateFunction(anchor).deprecated().orElseThrow();
    assertEquals("1.2.0", deprecation.since());
    assertTrue(deprecation.reason().isEmpty());
  }

  @Test
  void testWindowFunctionDeprecation() {
    SimpleExtension.FunctionAnchor anchor =
        SimpleExtension.FunctionAnchor.of(URN, "deprecatedWindow:i64");
    SimpleExtension.DeprecationStatus deprecation =
        extensions.getWindowFunction(anchor).deprecated().orElseThrow();
    assertEquals("1.3.0", deprecation.since());
  }

  @Test
  void testImplLevelDeprecation() {
    // Only the i64 overload of evolvingScalar is deprecated; the fp32 overload is not.
    SimpleExtension.DeprecationStatus deprecation =
        extensions
            .getScalarFunction(SimpleExtension.FunctionAnchor.of(URN, "evolvingScalar:i64"))
            .deprecated()
            .orElseThrow();
    assertEquals("3.1.0", deprecation.since());
    assertEquals("Use the fp32 overload instead", deprecation.reason().orElseThrow());

    assertTrue(
        extensions
            .getScalarFunction(SimpleExtension.FunctionAnchor.of(URN, "evolvingScalar:fp32"))
            .deprecated()
            .isEmpty());
  }

  @Test
  void testNonDeprecatedAggregateAsWindowFunction() {
    // Aggregate functions are also registered as window functions; deprecation must carry over.
    SimpleExtension.FunctionAnchor anchor =
        SimpleExtension.FunctionAnchor.of(URN, "deprecatedAggregate:i64");
    assertFalse(extensions.getWindowFunction(anchor).deprecated().isEmpty());
    assertEquals("1.2.0", extensions.getWindowFunction(anchor).deprecated().orElseThrow().since());
  }
}
