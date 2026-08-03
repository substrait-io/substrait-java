package io.substrait.docs;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.substrait.TestBase;
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.extension.AdvancedExtension;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.relation.NamedScan;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Backs the code samples in {@code docs/core/extensions.md}. Regions marked with {@code // --8<--
 * [start:name]} / {@code [end:name]} are pulled into the docs via {@code --8<--} snippet includes.
 */
class ExtensionsDocTest extends TestBase {

  @Test
  void defaultCollection() {
    // --8<-- [start:defaults]
    SimpleExtension.ExtensionCollection defaults = DefaultExtensionCatalog.DEFAULT_COLLECTION;
    // --8<-- [end:defaults]
    assertNotNull(defaults);
  }

  @Test
  void namedHelpers() {
    SubstraitBuilder b = new SubstraitBuilder();
    Expression colA = b.i32(1);
    Expression colB = b.i32(2);
    // --8<-- [start:named-helpers]
    // arithmetic and comparison helpers resolve FunctionAnchors internally
    b.add(b.i32(1), b.i32(2)); // add:i32_i32   in FUNCTIONS_ARITHMETIC
    b.equal(colA, colB); // equal:any_any in FUNCTIONS_COMPARISON
    // --8<-- [end:named-helpers]
  }

  @Test
  void genericFunctions() {
    SubstraitBuilder b = new SubstraitBuilder();
    Expression strArg = b.str("hello");
    Expression startArg = b.i32(1);
    Expression lengthArg = b.i32(3);
    NamedScan scan = b.namedScan(List.of("t"), List.of("a", "b"), List.of(R.I32, R.STRING));
    // --8<-- [start:generic-fns]
    // scalar: substring(str, start, length)
    Expression.ScalarFunctionInvocation substr =
        b.scalarFn(
            DefaultExtensionCatalog.FUNCTIONS_STRING,
            "substring:str_i32_i32",
            TypeCreator.REQUIRED.STRING,
            strArg,
            startArg,
            lengthArg);

    // aggregate: count(col)
    AggregateFunctionInvocation count =
        b.aggregateFn(
            DefaultExtensionCatalog.FUNCTIONS_AGGREGATE_GENERIC,
            "count:any",
            TypeCreator.REQUIRED.I64,
            b.fieldReference(scan, 0));
    // --8<-- [end:generic-fns]
    assertNotNull(substr);
    assertNotNull(count);
  }

  @Test
  void mergeCustomExtensions() {
    String yamlContent =
        "---\n"
            + "urn: extension:my.org:my_types\n"
            + "types:\n"
            + "  - name: point\n"
            + "    structure:\n"
            + "      x: i32\n"
            + "      y: i32\n";
    // --8<-- [start:merge]
    SimpleExtension.ExtensionCollection custom = SimpleExtension.load(yamlContent);

    SimpleExtension.ExtensionCollection combined =
        DefaultExtensionCatalog.DEFAULT_COLLECTION.merge(custom);

    SubstraitBuilder b = new SubstraitBuilder(combined);
    // --8<-- [end:merge]
    assertNotNull(combined);
    assertNotNull(b);
  }

  @Test
  void advancedExtension() {
    AdvancedExtension.Enhancement myEnhancement = new AdvancedExtension.Enhancement() {};
    AdvancedExtension.Optimization myOptimization = new AdvancedExtension.Optimization() {};
    // --8<-- [start:advanced-extension]
    AdvancedExtension ext =
        AdvancedExtension.builder()
            .enhancement(myEnhancement) // implements AdvancedExtension.Enhancement
            .addOptimizations(myOptimization) // implements AdvancedExtension.Optimization
            .build();
    // --8<-- [end:advanced-extension]
    assertNotNull(ext);
  }
}
