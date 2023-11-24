package io.substrait.isthmus;

import io.substrait.proto.ExtendedExpression;
import java.io.IOException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

public class SimpleExtendedExpressionsTest extends ExtendedExpressionTestBase {

  @Test
  public void filter() throws IOException, SqlParseException {
    ExtendedExpression extendedExpression =
        assertProtoExtendedExpressionRoundrip("L_ORDERKEY > 10");
  }

  @Test
  public void projection() throws IOException, SqlParseException {
    ExtendedExpression extendedExpression =
        assertProtoExtendedExpressionRoundrip("L_ORDERKEY + 10");
  }

  @Test
  public void in() throws IOException, SqlParseException {
    ExtendedExpression extendedExpression =
        assertProtoExtendedExpressionRoundrip("L_ORDERKEY IN (10, 20)");
  }

  @Test
  public void isNotNull() throws IOException, SqlParseException {
    ExtendedExpression extendedExpression =
        assertProtoExtendedExpressionRoundrip("L_ORDERKEY is not null");
  }

  @Test
  public void isNull() throws IOException, SqlParseException {
    ExtendedExpression extendedExpression =
        assertProtoExtendedExpressionRoundrip("L_ORDERKEY is null");
  }
}
