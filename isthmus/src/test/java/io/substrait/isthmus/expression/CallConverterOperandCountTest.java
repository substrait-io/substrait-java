package io.substrait.isthmus.expression;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.isthmus.PlanTestBase;
import java.math.BigDecimal;
import java.util.List;
import java.util.function.Function;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

/**
 * The converters for operators whose operands come in fixed groups reject a malformed {@link
 * RexCall} up front, instead of failing while reading the operands.
 */
class CallConverterOperandCountTest extends PlanTestBase {

  /** Never called: the converters reject these calls before converting any operand. */
  private static final Function<RexNode, Expression> OPERAND_CONVERTER =
      rex -> ExpressionCreator.i32(false, 0);

  private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
  private final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

  @Test
  void caseRejectsEvenOperandCount() {
    // A well-formed CASE has WHEN/THEN pairs plus an ELSE, so an odd number of operands.
    RexCall call = call(intType, SqlStdOperatorTable.CASE, rexBuilder.makeLiteral(true), one());

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> CallConverters.CASE.apply(call, OPERAND_CONVERTER));
    assertTrue(e.getMessage().contains("but got 2"), () -> "unexpected message: " + e.getMessage());
  }

  @Test
  void mapValueConstructorRejectsOddOperandCount() {
    RexCall call =
        call(
            typeFactory.createMapType(intType, intType),
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            one());

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> new SqlMapValueConstructorCallConverter().convert(call, OPERAND_CONVERTER));
    assertTrue(e.getMessage().contains("but got 1"), () -> "unexpected message: " + e.getMessage());
  }

  /**
   * Builds a call with the given return type, bypassing the return type inference that would reject
   * these operand counts before the converter sees them.
   */
  private RexCall call(
      RelDataType returnType, org.apache.calcite.sql.SqlOperator operator, RexNode... operands) {
    return (RexCall) rexBuilder.makeCall(returnType, operator, List.of(operands));
  }

  private RexNode one() {
    return rexBuilder.makeExactLiteral(BigDecimal.ONE, intType);
  }
}
