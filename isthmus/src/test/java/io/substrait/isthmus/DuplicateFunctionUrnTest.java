package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.substrait.expression.Expression;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction.Flag;
import org.junit.jupiter.api.Test;

/** Tests to reproduce #562 */
public class DuplicateFunctionUrnTest extends PlanTestBase {

  static final SimpleExtension.ExtensionCollection collection1;
  static final SimpleExtension.ExtensionCollection collection2;
  static final SimpleExtension.ExtensionCollection collection;

  static {
    try {
      String extensions1 = asString("extensions/functions_duplicate_urn1.yaml");
      String extensions2 = asString("extensions/functions_duplicate_urn2.yaml");
      collection1 = SimpleExtension.load("urn1://functions", extensions1);
      collection2 = SimpleExtension.load("urn2://functions", extensions2);
      collection = collection1.merge(collection2);

      // Verify that the merged collection contains duplicate concat functions with different URNs
      // This is a precondition for the tests - if this fails, the tests don't make sense
      List<SimpleExtension.ScalarFunctionVariant> concatFunctions =
          collection.scalarFunctions().stream().filter(f -> f.name().equals("concat")).toList();

      if (concatFunctions.size() != 2) {
        throw new IllegalStateException(
            "Expected 2 concat functions in merged collection, but found: "
                + concatFunctions.size());
      }

      String urn1 = concatFunctions.get(0).getAnchor().urn();
      String urn2 = concatFunctions.get(1).getAnchor().urn();
      if (urn1.equals(urn2)) {
        throw new IllegalStateException(
            "Expected different URNs for the two concat functions, but both were: " + urn1);
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

  @Test
  void testMergeOrderDeterminesFunctionPrecedence() {
    // This test verifies that when multiple extension collections contain functions with
    // the same name and signature but different URNs, the merge order determines precedence.
    // The FunctionConverter uses a "last-wins" strategy: the last function added to the
    // extension collection will be matched when converting from Calcite to Substrait.

    SimpleExtension.ExtensionCollection reverseCollection = collection2.merge(collection1);
    ScalarFunctionConverter converterA =
        new ScalarFunctionConverter(collection.scalarFunctions(), typeFactory);
    ScalarFunctionConverter converterB =
        new ScalarFunctionConverter(reverseCollection.scalarFunctions(), typeFactory);

    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RexCall concatCall =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.CONCAT,
                rexBuilder.makeLiteral("hello"),
                rexBuilder.makeLiteral("world"));

    // Create a simple topLevelConverter that converts literals to Substrait expressions
    java.util.function.Function<RexNode, Expression> topLevelConverter =
        rexNode -> {
          org.apache.calcite.rex.RexLiteral lit = (org.apache.calcite.rex.RexLiteral) rexNode;
          return Expression.StrLiteral.builder()
              .value(lit.getValueAs(String.class))
              .nullable(false)
              .build();
        };

    Optional<Expression> exprA = converterA.convert(concatCall, topLevelConverter);
    Optional<Expression> exprB = converterB.convert(concatCall, topLevelConverter);

    Expression.ScalarFunctionInvocation funcA = (Expression.ScalarFunctionInvocation) exprA.get();
    Expression.ScalarFunctionInvocation funcB = (Expression.ScalarFunctionInvocation) exprB.get();

    assertEquals(
        "extension:com.domain:string",
        funcA.declaration().getAnchor().urn(),
        "converterA should use last concat function (from collection2)");

    assertEquals(
        "extension:io.substrait:functions_string",
        funcB.declaration().getAnchor().urn(),
        "converterB should use last concat function (from collection1)");
  }

  @Test
  void testLtrimMergeOrderWithDefaultExtensions() {
    // This test verifies precedence between a custom ltrim (from collection2 with
    // extension:com.domain:string) and the default extension catalog's ltrim
    // (extension:io.substrait:functions_string).
    // The FunctionConverter uses a "last-wins" strategy.

    // Merge default extensions with collection2 - collection2's ltrim should be last
    SimpleExtension.ExtensionCollection defaultWithCustom = extensions.merge(collection2);

    // Merge collection2 with default extensions - default ltrim should be last
    SimpleExtension.ExtensionCollection customWithDefault = collection2.merge(extensions);

    ScalarFunctionConverter converterA =
        new ScalarFunctionConverter(defaultWithCustom.scalarFunctions(), typeFactory);
    ScalarFunctionConverter converterB =
        new ScalarFunctionConverter(customWithDefault.scalarFunctions(), typeFactory);

    // Create a TRIM(LEADING ' ' FROM 'test') call which uses TrimFunctionMapper to map to ltrim
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RexCall trimCall =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.TRIM,
                rexBuilder.makeFlag(Flag.LEADING),
                rexBuilder.makeLiteral(" "),
                rexBuilder.makeLiteral("test"));

    java.util.function.Function<RexNode, Expression> topLevelConverter =
        rexNode -> {
          org.apache.calcite.rex.RexLiteral lit = (org.apache.calcite.rex.RexLiteral) rexNode;
          Object value = lit.getValue();
          if (value == null) {
            return Expression.StrLiteral.builder().value("").nullable(true).build();
          }
          // Convert any literal value to string
          return Expression.StrLiteral.builder().value(value.toString()).nullable(false).build();
        };

    Optional<Expression> exprA = converterA.convert(trimCall, topLevelConverter);
    Optional<Expression> exprB = converterB.convert(trimCall, topLevelConverter);

    Expression.ScalarFunctionInvocation funcA = (Expression.ScalarFunctionInvocation) exprA.get();
    // converterA should use collection2's custom ltrim (last)
    assertEquals(
        "extension:com.domain:string",
        funcA.declaration().getAnchor().urn(),
        "converterA should use last ltrim (custom from collection2)");

    Expression.ScalarFunctionInvocation funcB = (Expression.ScalarFunctionInvocation) exprB.get();
    // converterB should use default extensions' ltrim (last)
    assertEquals(
        "extension:io.substrait:functions_string",
        funcB.declaration().getAnchor().urn(),
        "converterB should use last ltrim (from default extensions)");
  }
}
