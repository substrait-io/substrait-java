package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests to reproduce #562 */
public class DuplicateFunctionUrnTest extends PlanTestBase {

  static final SimpleExtension.ExtensionCollection collection;

  static {
    try {
      String extensions1 = asString("extensions/functions_duplicate_urn1.yaml");
      String extensions2 = asString("extensions/functions_duplicate_urn2.yaml");
      SimpleExtension.ExtensionCollection collection1 =
          SimpleExtension.load("urn1://functions", extensions1);
      SimpleExtension.ExtensionCollection collection2 =
          SimpleExtension.load("urn2://functions", extensions2);
      collection = collection1.merge(collection2);

      // Verify that the merged collection contains duplicate functions with different URNs
      // This is a precondition for the tests - if this fails, the tests don't make sense
      List<SimpleExtension.ScalarFunctionVariant> ltrimFunctions =
          collection.scalarFunctions().stream().filter(f -> f.name().equals("ltrim")).toList();

      if (ltrimFunctions.size() != 2) {
        throw new IllegalStateException(
            "Expected 2 ltrim functions in merged collection, but found: " + ltrimFunctions.size());
      }

      String urn1 = ltrimFunctions.get(0).getAnchor().urn();
      String urn2 = ltrimFunctions.get(1).getAnchor().urn();
      if (urn1.equals(urn2)) {
        throw new IllegalStateException(
            "Expected different URNs for the two ltrim functions, but both were: " + urn1);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Test
  void testDuplicateFunctionWithDifferentUrns() {
    ScalarFunctionConverter converter =
        new ScalarFunctionConverter(collection.scalarFunctions(), typeFactory);

    assertNotNull(converter);
  }

  @Test
  void testDuplicateAggregateFunctionWithDifferentUrns() {
    AggregateFunctionConverter converter =
        new AggregateFunctionConverter(collection.aggregateFunctions(), typeFactory);

    assertNotNull(converter);
  }

  @Test
  void testDuplicateWindowFunctionWithDifferentUrns() {
    WindowFunctionConverter converter =
        new WindowFunctionConverter(collection.windowFunctions(), typeFactory);

    assertNotNull(converter);
  }
}
