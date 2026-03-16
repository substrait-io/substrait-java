package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.LambdaBuilder;
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
}
