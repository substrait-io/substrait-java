package io.substrait.relation;

import io.substrait.TestBase;
import io.substrait.expression.ExpressionCreator;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class JoinTest extends TestBase {

  final Rel leftTable =
      sb.namedScan(
          Arrays.asList("T1"),
          Arrays.asList("a", "b", "c"),
          Arrays.asList(R.I64, R.FP64, R.STRING));

  final Rel rightTable =
      sb.namedScan(
          Arrays.asList("T2"),
          Arrays.asList("d", "e", "f"),
          Arrays.asList(R.FP64, R.STRING, R.I64));

  @Test
  void leftMarkJoinRoundtrip() {
    Join join =
        Join.builder()
            .left(leftTable)
            .right(rightTable)
            .condition(ExpressionCreator.bool(false, true))
            .joinType(Join.JoinType.LEFT_MARK)
            .build();
    verifyRoundTrip(join);
  }

  @Test
  void rightMarkJoinRoundtrip() {
    Join join =
        Join.builder()
            .left(leftTable)
            .right(rightTable)
            .condition(ExpressionCreator.bool(false, true))
            .joinType(Join.JoinType.RIGHT_MARK)
            .build();
    verifyRoundTrip(join);
  }
}
