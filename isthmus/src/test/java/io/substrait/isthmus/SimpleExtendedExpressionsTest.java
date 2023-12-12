package io.substrait.isthmus;

import io.substrait.proto.ExtendedExpression;
import java.io.IOException;
import java.util.stream.Stream;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class SimpleExtendedExpressionsTest extends ExtendedExpressionTestBase {

  private static Stream<Arguments> expressionTypeProvider() {
    return Stream.of(
        Arguments.of("2"), // I32LiteralExpression
        Arguments.of("L_ORDERKEY"), // FieldReferenceExpression
        Arguments.of("L_ORDERKEY > 10"), // ScalarFunctionExpressionFilter
        Arguments.of("L_ORDERKEY + 10"), // ScalarFunctionExpressionProjection
        Arguments.of("L_ORDERKEY IN (10, 20)"), // ScalarFunctionExpressionIn
        Arguments.of("L_ORDERKEY is not null"), // ScalarFunctionExpressionIsNotNull
        Arguments.of("L_ORDERKEY is null") // ScalarFunctionExpressionIsNull
        );
  }

  @ParameterizedTest
  @MethodSource("expressionTypeProvider")
  public void testExtendedExpressionsRoundTrip(String sqlExpression)
      throws SqlParseException, IOException {

    ExtendedExpression extendedExpression = assertProtoExtendedExpressionRoundrip(sqlExpression);
  }
}
