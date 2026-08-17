package io.substrait.type.proto;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.relation.Rel;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

/**
 * Round-trips the expression types that embed a relation, so that the ProtoRelConverter the
 * ProtoExpressionConverter delegates subqueries to is exercised through the shared
 * TestBase#verifyRoundTrip(Expression) helper.
 */
class SubqueryExpressionRoundtripTest extends TestBase {

  final Rel subqueryInput =
      sb.namedScan(
          Arrays.asList("subquery_table"), Arrays.asList("subquery_column"), Arrays.asList(R.I64));

  @Test
  void scalarSubquery() {
    verifyRoundTrip(Expression.ScalarSubquery.builder().input(subqueryInput).type(R.I64).build());
  }

  @Test
  void setPredicate() {
    verifyRoundTrip(
        Expression.SetPredicate.builder()
            .predicateOp(Expression.PredicateOp.PREDICATE_OP_EXISTS)
            .tuples(subqueryInput)
            .build());
  }

  @Test
  void inPredicate() {
    verifyRoundTrip(
        Expression.InPredicate.builder()
            .haystack(subqueryInput)
            .needles(Collections.singletonList(sb.i64(42)))
            .build());
  }
}
