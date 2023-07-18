package io.substrait.isthmus;

import static io.substrait.isthmus.expression.CallConverters.CREATE_SEARCH_CONV;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.plan.Plan;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.util.List;
import org.apache.calcite.rel.core.Filter;
import org.junit.jupiter.api.Test;

/** Tests which test that an expression round-trips correctly to and from Calcite expressions. */
public class ExpressionRoundtripTest extends PlanTestBase {
  static final TypeCreator R = TypeCreator.of(false);
  static final TypeCreator N = TypeCreator.of(true);

  final SubstraitBuilder b = new SubstraitBuilder(extensions);

  // Define a shared table (i.e. a NamedScan) for use in tests.
  final List<Type> commonTableType = List.of(R.I32, R.FP32, N.STRING, N.BOOLEAN);
  final Rel commonTable =
      b.namedScan(List.of("example"), List.of("a", "b", "c", "d"), commonTableType);

  final SubstraitToCalcite converter = new SubstraitToCalcite(extensions, typeFactory);

  @Test
  public void singleOrList() throws IOException {
    Plan.Root root =
        b.root(
            b.filter(
                input -> b.singleOrList(b.fieldReference(input, 0), b.i32(5), b.i32(10)),
                commonTable));
    var relNode = converter.convert(root.getInput());
    var expression =
        ((Filter) relNode)
            .getCondition()
            .accept(
                new RexExpressionConverter(
                    CREATE_SEARCH_CONV.apply(relNode.getCluster().getRexBuilder()),
                    new ScalarFunctionConverter(
                        SimpleExtension.loadDefaults().scalarFunctions(), typeFactory)));
    var to = new ExpressionProtoConverter(new ExtensionCollector(), null);
    assertEquals(
        expression.accept(to),
        b.scalarFn(
                "/functions_boolean.yaml",
                "or:bool",
                R.BOOLEAN,
                b.scalarFn(
                    "/functions_comparison.yaml",
                    "equal:any_any",
                    R.BOOLEAN,
                    b.fieldReference(commonTable, 0),
                    b.i32(5)),
                b.scalarFn(
                    "/functions_comparison.yaml",
                    "equal:any_any",
                    R.BOOLEAN,
                    b.fieldReference(commonTable, 0),
                    b.i32(10)))
            .accept(to));
  }
}
