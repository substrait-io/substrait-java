package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.substrait.expression.Expression;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.plan.Plan;
import io.substrait.relation.Project;
import io.substrait.type.Type;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * The operands of a converted call are cast just far enough for the function's declaration to bind
 * them: each of its parameters has to take one value from the argument types.
 */
class OperandCoercionTest extends PlanTestBase {

  private static final String CREATES =
      "CREATE TABLE t (d7 DECIMAL(7, 2) NOT NULL, d5 DECIMAL(5, 2) NOT NULL, "
          + "i INT NOT NULL, c CHAR(10) NOT NULL)";

  /** gte(any1, any1) binds any1 once, so decimals of two precisions both take the wider one. */
  @Test
  void aComparisonOfTwoDecimalsCastsBothToOneType() throws Exception {
    Expression.ScalarFunctionInvocation between = call("SELECT d7 BETWEEN 0.99 AND 1.49 FROM t");
    Expression.ScalarFunctionInvocation call =
        assertInstanceOf(Expression.ScalarFunctionInvocation.class, between.arguments().get(0));

    assertEquals("gte:any_any", call.declaration().key());
    assertEquals(List.of(R.decimal(7, 2), R.decimal(7, 2)), argumentTypes(call));
    assertBinds(call);
  }

  /**
   * multiply(decimal<P1,S1>, decimal<P2,S2>) binds each operand's own precision, so an integer
   * operand becomes the decimal that holds it, not the other operand's decimal.
   */
  @Test
  void aDecimalTimesAnIntegerCastsTheIntegerToItsOwnDecimal() throws Exception {
    Expression.ScalarFunctionInvocation call = call("SELECT d7 * i FROM t");

    assertEquals("multiply:dec_dec", call.declaration().key());
    assertEquals(List.of(R.decimal(7, 2), R.decimal(10, 0)), argumentTypes(call));
    assertBinds(call);
  }

  /** A varchar declaration does not bind a char, so a char(n) operand becomes a varchar(n). */
  @Test
  void aCharOperandOfAVarcharFunctionBecomesAVarchar() throws Exception {
    Expression.ScalarFunctionInvocation call = call("SELECT c LIKE 'a%' FROM t");

    assertEquals("like:vchar_vchar", call.declaration().key());
    assertEquals(R.varChar(10), argumentTypes(call).get(0));
    assertBinds(call);
  }

  private static Expression.ScalarFunctionInvocation call(String query) throws Exception {
    Plan plan =
        new SqlToSubstrait()
            .convert(
                query, SubstraitCreateStatementParser.processCreateStatementsToCatalog(CREATES));
    Expression expression = ((Project) plan.getRoots().get(0).getInput()).getExpressions().get(0);
    return assertInstanceOf(Expression.ScalarFunctionInvocation.class, expression);
  }

  private static List<Type> argumentTypes(Expression.ScalarFunctionInvocation call) {
    return call.arguments().stream()
        .filter(Expression.class::isInstance)
        .map(argument -> ((Expression) argument).getType())
        .collect(Collectors.toList());
  }

  private static void assertBinds(Expression.ScalarFunctionInvocation call) {
    assertDoesNotThrow(() -> call.declaration().resolveType(argumentTypes(call)));
  }
}
