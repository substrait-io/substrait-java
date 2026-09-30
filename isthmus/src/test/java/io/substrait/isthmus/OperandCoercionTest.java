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
      "CREATE TABLE t (d7 DECIMAL(7, 2) NOT NULL, i INT NOT NULL, c CHAR(10) NOT NULL, "
          + "v VARCHAR NOT NULL, v10 VARCHAR(10) NOT NULL)";

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
   * operand becomes the decimal that holds it, not the least restrictive decimal(12,2).
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

  /**
   * TPC-H declares l_discount as a bare DECIMAL, which is decimal(38,0). No decimal holds both it
   * and 0.02 exactly, so the operands are left as they are rather than cast to a type that drops
   * the fraction and turns the bound into 0.
   */
  @Test
  void noOperandIsCastToADecimalThatLosesItsScale() throws Exception {
    Plan plan =
        new SqlToSubstrait()
            .convert(
                "SELECT l_discount BETWEEN 0.03 - 0.01 AND 0.03 + 0.01 FROM lineitem",
                TPCH_CATALOG);
    Expression.ScalarFunctionInvocation between =
        assertInstanceOf(
            Expression.ScalarFunctionInvocation.class,
            ((Project) plan.getRoots().get(0).getInput()).getExpressions().get(0));
    Expression.ScalarFunctionInvocation gte =
        assertInstanceOf(Expression.ScalarFunctionInvocation.class, between.arguments().get(0));

    Type.Decimal bound = assertInstanceOf(Type.Decimal.class, argumentTypes(gte).get(1));
    assertEquals(2, bound.scale());
  }

  /**
   * concat:vchar declares one operand's length as its result, so two char(10) operands would come
   * out as a varchar(10). The next variant, concat:str, takes them as strings.
   */
  @Test
  void aConcatenationNeverBindsADeclarationThatTruncatesIt() throws Exception {
    Expression.ScalarFunctionInvocation call = call("SELECT c || c FROM t");

    assertEquals("concat:str", call.declaration().key());
    assertEquals(List.of(R.STRING, R.STRING), argumentTypes(call));
    assertBinds(call);
  }

  /**
   * like:vchar_vchar does not bind an unbounded varchar, which is a string, so the next variant,
   * like:str_str, is tried and binds once the pattern is a string too.
   */
  @Test
  void aVariantThatDoesNotBindGivesWayToTheNextOne() throws Exception {
    Expression.ScalarFunctionInvocation call = call("SELECT v LIKE 'a%' FROM t");

    assertEquals("like:str_str", call.declaration().key());
    assertEquals(List.of(R.STRING, R.STRING), argumentTypes(call));
    assertBinds(call);
  }

  /** A declaration's concrete argument type is bound exactly: replace:str_str_str takes strings. */
  @Test
  void aConcretelyDeclaredStringTakesAString() throws Exception {
    Expression.ScalarFunctionInvocation call = call("SELECT REPLACE(c, v10, v10) FROM t");

    assertEquals("replace:str_str_str", call.declaration().key());
    assertEquals(List.of(R.STRING, R.STRING, R.STRING), argumentTypes(call));
  }

  private Expression.ScalarFunctionInvocation call(String query) throws Exception {
    Plan plan =
        toSubstraitPlan(
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
