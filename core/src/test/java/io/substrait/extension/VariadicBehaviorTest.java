package io.substrait.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/** Tests for VariadicBehavior, particularly parameterConsistency loading from YAML. */
class VariadicBehaviorTest {

  @Test
  void testParameterConsistencyLoading() {
    String yamlContent =
        "urn: extension:test:example\n"
            + "scalar_functions:\n"
            + "  - name: test_func\n"
            + "    impls:\n"
            + "      - args:\n"
            + "          - name: arg1\n"
            + "            value: string\n"
            + "        variadic:\n"
            + "          min: 1\n"
            + "          parameterConsistency: CONSISTENT\n"
            + "        return: string\n";

    SimpleExtension.ExtensionCollection collection =
        SimpleExtension.load("test://example", yamlContent);

    assertEquals(
        SimpleExtension.VariadicBehavior.ParameterConsistency.CONSISTENT,
        collection.scalarFunctions().get(0).variadic().get().parameterConsistency());
  }
}

