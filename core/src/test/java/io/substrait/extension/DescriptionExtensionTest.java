package io.substrait.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.junit.jupiter.api.Test;

/**
 * Verifies that descriptions can be read from extension YAML files at both the function level and
 * the function-implementation level. An implementation-level description documents
 * overload-specific behavior and takes precedence over the parent function's description when both
 * are present.
 *
 * <p>See <a href="https://github.com/substrait-io/substrait/pull/1013">substrait#1013</a>.
 */
class DescriptionExtensionTest extends TestBase {

  static final String URN = "extension:test:description_extensions";
  static final SimpleExtension.ExtensionCollection DESCRIPTION_EXTENSION;

  static {
    try {
      String extensionStr = asString("extensions/description_extensions.yaml");
      DESCRIPTION_EXTENSION = SimpleExtension.load(extensionStr);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  DescriptionExtensionTest() {
    super(DESCRIPTION_EXTENSION);
  }

  @Test
  void testFunctionLevelDescriptionInherited() {
    // The single impl has no description of its own, so it inherits the function-level description.
    SimpleExtension.FunctionAnchor anchor =
        SimpleExtension.FunctionAnchor.of(URN, "documentedScalar:i64");
    assertEquals(
        "Function-level scalar description", extensions.getScalarFunction(anchor).description());
  }

  @Test
  void testImplLevelDescriptionTakesPrecedence() {
    // The i64 overload defines its own description; it must win over the function-level one.
    SimpleExtension.FunctionAnchor anchor =
        SimpleExtension.FunctionAnchor.of(URN, "overloadDocumented:i64");
    assertEquals(
        "Overload-specific description for i64",
        extensions.getScalarFunction(anchor).description());
  }

  @Test
  void testOverloadWithoutDescriptionInheritsFunctionLevel() {
    // The fp32 overload has no description of its own, so it inherits the function-level one.
    SimpleExtension.FunctionAnchor anchor =
        SimpleExtension.FunctionAnchor.of(URN, "overloadDocumented:fp32");
    assertEquals(
        "Shared function-level description", extensions.getScalarFunction(anchor).description());
  }

  @Test
  void testImplLevelDescriptionWithoutFunctionLevel() {
    SimpleExtension.FunctionAnchor anchor =
        SimpleExtension.FunctionAnchor.of(URN, "implOnlyScalar:i64");
    assertEquals("Only the impl is documented", extensions.getScalarFunction(anchor).description());
  }

  @Test
  void testAggregateImplLevelDescription() {
    SimpleExtension.FunctionAnchor anchor =
        SimpleExtension.FunctionAnchor.of(URN, "documentedAggregate:i64");
    assertEquals(
        "Aggregate impl description", extensions.getAggregateFunction(anchor).description());
  }

  @Test
  void testAggregateImplDescriptionCarriesOverToWindowFunction() {
    // Aggregate functions are also registered as window functions; the description must carry over.
    SimpleExtension.FunctionAnchor anchor =
        SimpleExtension.FunctionAnchor.of(URN, "documentedAggregate:i64");
    assertEquals("Aggregate impl description", extensions.getWindowFunction(anchor).description());
  }

  @Test
  void testWindowImplLevelDescriptionTakesPrecedence() {
    SimpleExtension.FunctionAnchor anchor =
        SimpleExtension.FunctionAnchor.of(URN, "documentedWindow:i64");
    assertEquals("Window impl description", extensions.getWindowFunction(anchor).description());
  }
}
