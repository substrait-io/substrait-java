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
    RexNode arg1 = rexBuilder.makeLiteral("hello");
    RexNode arg2 = rexBuilder.makeLiteral("world");
    RexCall concatCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.CONCAT, arg1, arg2);

    // Create a simple topLevelConverter that converts literals to Substrait expressions
    java.util.function.Function<RexNode, Expression> topLevelConverter =
        rexNode -> {
          if (rexNode instanceof org.apache.calcite.rex.RexLiteral) {
            org.apache.calcite.rex.RexLiteral lit = (org.apache.calcite.rex.RexLiteral) rexNode;
            return Expression.StrLiteral.builder()
                .value(lit.getValueAs(String.class))
                .nullable(false)
                .build();
          }
          throw new UnsupportedOperationException("Only literals supported in test");
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
}
