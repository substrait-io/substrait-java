package io.substrait.relation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.expression.ExpressionCreator;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
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

  /**
   * Per the Substrait spec, a LEFT MARK join emits every left row with a nullable boolean "mark"
   * column appended at the end. The mark is nullable because the match state is 3-valued: true (a
   * partner matched), false (no partner, no NULL comparisons) or NULL (no partner but some
   * comparison was NULL).
   */
  @Test
  void leftMarkJoinRecordType() {
    Join join =
        Join.builder()
            .left(leftTable)
            .right(rightTable)
            .condition(ExpressionCreator.bool(false, true))
            .joinType(Join.JoinType.LEFT_MARK)
            .build();

    Type.Struct expected =
        TypeCreator.REQUIRED.struct(R.I64, R.FP64, R.STRING, TypeCreator.NULLABLE.BOOLEAN);
    assertEquals(expected, join.getRecordType());
  }

  /**
   * Per the Substrait spec, a RIGHT MARK join emits every right row with a nullable boolean "mark"
   * column appended at the end.
   */
  @Test
  void rightMarkJoinRecordType() {
    Join join =
        Join.builder()
            .left(leftTable)
            .right(rightTable)
            .condition(ExpressionCreator.bool(false, true))
            .joinType(Join.JoinType.RIGHT_MARK)
            .build();

    Type.Struct expected =
        TypeCreator.REQUIRED.struct(R.FP64, R.STRING, R.I64, TypeCreator.NULLABLE.BOOLEAN);
    assertEquals(expected, join.getRecordType());
  }

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
