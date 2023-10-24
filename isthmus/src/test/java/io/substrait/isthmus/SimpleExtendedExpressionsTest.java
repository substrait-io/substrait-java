package io.substrait.isthmus;

import java.io.IOException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

public class SimpleExtendedExpressionsTest extends ExtendedExpressionTestBase {

  @Test
  public void filter() throws IOException, SqlParseException {
    assertProtoExtendedExpressionRoundrip("N_NATIONKEY > 18");
  }
}
