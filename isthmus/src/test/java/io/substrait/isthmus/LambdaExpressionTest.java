package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.LambdaBuilder;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class LambdaExpressionTest extends PlanTestBase {

  final Rel emptyTable = sb.emptyVirtualTableScan();
  final LambdaBuilder lb = new LambdaBuilder();

  // () -> 42
  @Test
  void lambdaExpressionZeroParameters() {
    List<Expression> exprs = new ArrayList<>();
    exprs.add(lb.lambda(List.of(), params -> ExpressionCreator.i32(false, 42)));

    Project project = Project.builder().expressions(exprs).input(emptyTable).build();
    assertFullRoundTrip(project);
  }

  // (x: i32, y: i64, z: string) -> x
  @Test
  void validFieldIndex() {
    List<Expression> exprs = new ArrayList<>();
    exprs.add(lb.lambda(List.of(R.I32, R.I64, R.STRING), params -> params.ref(0)));

    Project project = Project.builder().expressions(exprs).input(emptyTable).build();
    assertFullRoundTrip(project);
  }

  // (x: i32) -> 42
  @Test
  void lambdaWithLiteralBody() {
    List<Expression> exprs = new ArrayList<>();
    exprs.add(lb.lambda(List.of(R.I32), params -> ExpressionCreator.i32(false, 42)));

    Project project = Project.builder().expressions(exprs).input(emptyTable).build();
    assertFullRoundTrip(project);
  }

  // (x: i64) -> (y: i32) -> x — Calcite doesn't support nested lambdas
  @Test
  void nestedLambdaThrowsUnsupportedOperation() {
    Expression.Lambda outerLambda =
        lb.lambda(List.of(R.I64), outer -> lb.lambda(List.of(R.I32), inner -> outer.ref(0)));

    List<Expression> exprs = new ArrayList<>();
    exprs.add(outerLambda);
    Project project = Project.builder().expressions(exprs).input(emptyTable).build();
    assertThrows(UnsupportedOperationException.class, () -> assertFullRoundTrip(project));
  }

  // (x: i64) -> (y1: i64, y2: i64) -> y1 * x + y2
  @Test
  void nestedLambdaWithArithmeticBody() {
    String ARITH = DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC;

    Expression.Lambda lambda =
        lb.lambda(
            List.of(R.I64),
            outer ->
                lb.lambda(
                    List.of(R.I64, R.I64),
                    inner -> {
                      Expression multiply =
                          sb.scalarFn(ARITH, "multiply:i64_i64", R.I64, inner.ref(0), outer.ref(0));
                      return sb.scalarFn(ARITH, "add:i64_i64", R.I64, multiply, inner.ref(1));
                    }));

    // Proto-only roundtrip since Calcite doesn't support nested lambdas
    List<Expression> exprs = new ArrayList<>();
    exprs.add(lambda);
    Project project = Project.builder().expressions(exprs).input(emptyTable).build();

    io.substrait.extension.ExtensionCollector collector =
        new io.substrait.extension.ExtensionCollector();
    io.substrait.proto.Rel proto =
        new io.substrait.relation.RelProtoConverter(collector).toProto(project);
    io.substrait.relation.Rel roundTripped =
        new io.substrait.relation.ProtoRelConverter(collector, extensions).from(proto);
    assertEquals(project, roundTripped);
  }
}
