package io.substrait.docs;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.substrait.TestBase;
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.extension.SimpleExtension;
import io.substrait.plan.Plan;
import io.substrait.relation.Filter;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Backs the code samples in {@code docs/core/building-plans.md}. Each region marked with {@code //
 * --8<-- [start:name]} / {@code [end:name]} is pulled into the docs via a {@code --8<--} snippet
 * include, so the samples shown there are exactly this compiled and executed code.
 */
class BuildingPlansDocTest extends TestBase {

  @Test
  void creatingABuilder() {
    // --8<-- [start:create-builder]
    SubstraitBuilder b = new SubstraitBuilder();
    // --8<-- [end:create-builder]
    assertNotNull(b);
  }

  @Test
  void creatingABuilderWithACustomCollection() {
    SimpleExtension.ExtensionCollection myExtensionCollection = extensions;
    // --8<-- [start:create-builder-custom]
    SubstraitBuilder b = new SubstraitBuilder(myExtensionCollection);
    // --8<-- [end:create-builder-custom]
    assertNotNull(b);
  }

  @Test
  void typeShortcuts() {
    // --8<-- [start:type-shortcuts]
    TypeCreator R = TypeCreator.REQUIRED; // non-nullable types
    TypeCreator N = TypeCreator.NULLABLE; // nullable types
    // --8<-- [end:type-shortcuts]
    assertNotNull(R.I32);
    assertNotNull(N.STRING);
  }

  @Test
  void relationHelpersTakeLambdas() {
    SubstraitBuilder b = new SubstraitBuilder();
    // --8<-- [start:filter-lambda]
    NamedScan scan = b.namedScan(List.of("t"), List.of("a", "b"), List.of(R.I32, R.STRING));

    Filter filter = b.filter(rel -> b.equal(b.fieldReference(rel, 0), b.i32(10)), scan);
    // --8<-- [end:filter-lambda]
    assertNotNull(filter);
  }

  @Test
  void endToEndExample() {
    // --8<-- [start:end-to-end]
    TypeCreator R = TypeCreator.REQUIRED;
    SubstraitBuilder b = new SubstraitBuilder();

    // 1. scan: table "t" with columns a (i32) and b (string)
    NamedScan scan = b.namedScan(List.of("t"), List.of("a", "b"), List.of(R.I32, R.STRING));

    // 2. filter: keep rows where a == 10
    Filter filter = b.filter(rel -> b.equal(b.fieldReference(rel, 0), b.i32(10)), scan);

    // 3. project: output columns a and b. project appends its expressions after
    //    the input fields, so remap keeps only the two projected columns.
    Project project =
        b.project(
            rel -> List.of(b.fieldReference(rel, 0), b.fieldReference(rel, 1)),
            b.remap(2, 3),
            filter);

    // 4. root: name the plan's output columns
    Plan.Root root = b.root(project, List.of("a", "b"));

    // 5. plan: wrap the root
    Plan plan = b.plan(root);
    // --8<-- [end:end-to-end]
    assertNotNull(plan);
  }
}
