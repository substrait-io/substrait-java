package io.substrait.type.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.function.ParameterizedType;
import io.substrait.function.ParameterizedTypeCreator;
import io.substrait.function.TypeExpression;
import io.substrait.function.TypeExpressionCreator;
import io.substrait.type.ImmutableType;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.junit.jupiter.api.Test;

class TestTypeParser {

  private final TypeCreator n = TypeCreator.NULLABLE;
  private final TypeCreator r = TypeCreator.REQUIRED;

  private final TypeExpressionCreator eo = TypeExpressionCreator.REQUIRED;
  private final ParameterizedTypeCreator pr = ParameterizedTypeCreator.REQUIRED;
  private final ParameterizedTypeCreator pn = ParameterizedTypeCreator.NULLABLE;

  private static final String URN = "test";

  @Test
  void basic() {
    simpleTests(ParseToPojo.Visitor.simple(URN));
  }

  @Test
  void compound() {
    compoundTests(ParseToPojo.Visitor.simple(URN));
  }

  @Test
  void parameterizedSimple() {
    simpleTests(ParseToPojo.Visitor.parameterized(URN));
  }

  @Test
  void parameterizedCompound() {
    compoundTests(ParseToPojo.Visitor.parameterized(URN));
  }

  @Test
  void parameterizedParameterized() {
    parameterizedTests(ParseToPojo.Visitor.parameterized(URN));
  }

  @Test
  void derivationSimple() {
    simpleTests(ParseToPojo.Visitor.expression(URN));
  }

  @Test
  void derivationCompound() {
    compoundTests(ParseToPojo.Visitor.expression(URN));
  }

  @Test
  void derivationParameterized() {
    parameterizedTests(ParseToPojo.Visitor.expression(URN));
  }

  @Test
  void derivationExpression() {
    test(
        ParseToPojo.Visitor.expression(URN),
        eo.fixedCharE(eo.plus(pr.parameter("L1"), pr.parameter("L2"))),
        "FIXEDCHAR<L1+L2>");
    test(
        ParseToPojo.Visitor.expression(URN),
        eo.program(pr.fixedCharE("L1"), new TypeExpressionCreator.Assign("L1", eo.i(1))),
        "L1=1\nFIXEDCHAR<L1>");
  }

  @Test
  void operatorPrecedence() {
    // Binary operators must bind with conventional precedence (spec v0.95.0 grammar fix):
    // '*' '/' tighter than '+' '-', tighter than comparisons, then 'and', then 'or'. Parentheses
    // override. Each case is rendered fully parenthesized from the parsed expression tree.
    assertPrecedence("1 + 2 * 3", "(1 + (2 * 3))");
    assertPrecedence("1 + 2 * 3 - 4 / 5", "((1 + (2 * 3)) - (4 / 5))");
    assertPrecedence("1 * 2 + 3", "((1 * 2) + 3)");
    assertPrecedence("(1 + 2) * 3", "((1 + 2) * 3)");
    assertPrecedence("1 + 2 < 3 * 4", "((1 + 2) < (3 * 4))");
    assertPrecedence("a and b or c", "((a and b) or c)");
  }

  private static void assertPrecedence(String toParse, String expected) {
    TypeExpression parsed = TypeStringParser.parse(toParse, ParseToPojo.Visitor.expression(URN));
    assertEquals(expected, render(parsed), toParse);
  }

  /** Renders a parsed type expression as a fully-parenthesized string reflecting its structure. */
  private static String render(TypeExpression e) {
    if (e instanceof TypeExpression.BinaryOperation) {
      TypeExpression.BinaryOperation op = (TypeExpression.BinaryOperation) e;
      return "(" + render(op.left()) + " " + symbol(op.opType()) + " " + render(op.right()) + ")";
    }
    if (e instanceof TypeExpression.IntegerLiteral) {
      return Integer.toString(((TypeExpression.IntegerLiteral) e).value());
    }
    if (e instanceof ParameterizedType.StringLiteral) {
      return ((ParameterizedType.StringLiteral) e).value();
    }
    throw new IllegalStateException("Unexpected type expression: " + e);
  }

  private static String symbol(TypeExpression.BinaryOperation.OpType op) {
    switch (op) {
      case ADD:
        return "+";
      case SUBTRACT:
        return "-";
      case MULTIPLY:
        return "*";
      case DIVIDE:
        return "/";
      case LT:
        return "<";
      case GT:
        return ">";
      case EQ:
        return "=";
      case AND:
        return "and";
      case OR:
        return "or";
      default:
        throw new IllegalStateException("Unexpected operator: " + op);
    }
  }

  private <T> void simpleTests(ParseToPojo.Visitor v) {
    test(v, r.I8, "I8");
    test(v, r.I16, "I16");
    test(v, r.I32, "I32");
    test(v, r.I64, "I64");
    test(v, r.FP32, "FP32");
    test(v, r.FP64, "FP64");
    test(v, r.userDefined(URN, "foo"), "u!foo");

    // Nullable
    test(v, n.I8, "I8?");
    test(v, n.I16, "I16?");
    test(v, n.I32, "i32?");
    test(v, n.I64, "i64?");
    test(v, n.FP32, "FP32?");
    test(v, n.FP64, "FP64?");
    test(v, n.userDefined(URN, "foo"), "u!foo?");
  }

  private void compoundTests(ParseToPojo.Visitor v) {
    test(v, r.fixedChar(1), "FIXEDCHAR<1>");
    test(v, r.varChar(2), "VARCHAR<2>");
    test(v, r.fixedBinary(3), "FIXEDBINARY<3>");
    test(v, r.decimal(1, 2), "DECIMAL<1, 2>");
    test(v, r.struct(r.I8, r.I16), "STRUCT<i8, i16>");
    test(v, r.list(r.I8), "LIST<i8>");
    test(v, r.map(r.I16, r.I8), "MAP<i16, i8>");

    // Nullable
    test(v, n.fixedChar(1), "FIXEDCHAR?<1>");
    test(v, n.varChar(2), "VARCHAR?<2>");
    test(v, n.fixedBinary(3), "FIXEDBINARY?<3>");
    test(v, n.decimal(1, 2), "DECIMAL?<1, 2>");
    test(v, n.struct(r.I8, n.I16), "STRUCT?<i8, i16?>");
    test(v, n.list(n.I8), "LIST?<i8?>");
    test(v, n.map(r.I16, n.I8), "MAP?<i16, i8?>");

    test(v, r.intervalDay(6), "INTERVAL_DAY<6>");
    test(v, n.intervalDay(9), "INTERVAL_DAY?<9>");
    test(v, r.intervalCompound(6), "INTERVAL_COMPOUND<6>");
    test(v, n.intervalCompound(9), "INTERVAL_COMPOUND?<9>");
    test(v, r.precisionTime(6), "PRECISION_TIME<6>");
    test(v, n.precisionTime(9), "PRECISION_TIME?<9>");
    test(v, r.precisionTimestamp(6), "PRECISION_TIMESTAMP<6>");
    test(v, n.precisionTimestamp(9), "PRECISION_TIMESTAMP?<9>");
    test(v, r.precisionTimestampTZ(6), "PRECISION_TIMESTAMP_TZ<6>");
    test(v, n.precisionTimestampTZ(9), "PRECISION_TIMESTAMP_TZ?<9>");

    test(v, r.func(List.of(r.I8), r.I32), "func<i8 -> i32>");
    test(v, r.func(List.of(r.I8, r.I8), r.I32), "func<(i8, i8) -> i32>");
    test(v, n.func(List.of(r.I8), r.I32), "func?<i8 -> i32>");
    test(v, r.func(List.of(n.I8), n.I32), "func<i8? -> i32?>");
  }

  private <T> void parameterizedTests(ParseToPojo.Visitor v) {
    test(v, pn.listE(pr.parameter("K")), "List?<K>");
    test(v, pr.structE(r.I8, r.I16, n.I8, pr.parameter("K")), "STRUCT<i8, i16, i8?, K>");
    test(v, pr.parameter("any"), "any");
    test(v, pn.parameter("any"), "any?");
    test(v, pr.parameter("any1"), "any1");
    test(v, pn.parameter("any1"), "any1?");
    test(v, pn.listE(pr.parameter("any")), "list?<any>");
    test(v, pn.listE(pn.parameter("any")), "list?<any?>");
    test(v, pn.structE(r.I8, r.I16, n.I8, pr.parameter("K")), "STRUCT?<i8, i16, i8?, K>");
    test(v, pr.decimalE("P", "S"), "DECIMAL<P, S>");
    test(v, pr.decimalE("P", "0"), "DECIMAL<P, 0>");
    test(v, pr.decimalE("14", "S"), "DECIMAL<14, S>");

    test(v, pr.intervalDayE("P"), "INTERVAL_DAY<P>");
    test(v, pr.intervalCompoundE("P"), "INTERVAL_COMPOUND<P>");
    test(v, pn.precisionTimeE("P"), "PRECISION_TIME?<P>");
    test(v, pr.precisionTimeE("P"), "PRECISION_TIME<P>");
    test(v, pr.precisionTimestampE("P"), "PRECISION_TIMESTAMP<P>");
    test(v, pr.precisionTimestampTZE("P"), "PRECISION_TIMESTAMP_TZ<P>");

    test(v, pr.funcE(List.of(pr.parameter("any")), r.I64), "func<any -> i64>");
    test(v, pr.funcE(List.of(pr.parameter("any"), r.I64), r.I64), "func<(any, i64) -> i64>");
    test(v, pr.funcE(List.of(pr.parameter("any1")), pr.parameter("any1")), "func<any1 -> any1>");
    test(v, pn.funcE(List.of(pr.parameter("any")), n.I64), "func?<any -> i64?>");
    test(v, pn.funcE(List.of(pr.parameter("any1")), pr.parameter("any1")), "func?<any1 -> any1>");
    test(
        v,
        pr.funcE(List.of(pr.parameter("any1"), r.I8), pr.parameter("any1")),
        "func<(any1, i8) -> any1>");
  }

  @Test
  void userDefinedWithTypeParams() {
    ParseToPojo.Visitor v = ParseToPojo.Visitor.simple(URN);
    test(
        v,
        Type.UserDefined.builder()
            .nullable(false)
            .urn(URN)
            .name("foo")
            .addTypeParameters(ImmutableType.ParameterDataType.builder().type(r.I8).build())
            .build(),
        "u!foo<i8>");
    test(
        v,
        Type.UserDefined.builder()
            .nullable(true)
            .urn(URN)
            .name("foo")
            .addTypeParameters(ImmutableType.ParameterDataType.builder().type(r.I8).build())
            .build(),
        "u!foo?<i8>");
    test(
        v,
        Type.UserDefined.builder()
            .nullable(false)
            .urn(URN)
            .name("foo")
            .addTypeParameters(ImmutableType.ParameterIntegerValue.builder().value(1).build())
            .build(),
        "u!foo<1>");
    test(
        v,
        Type.UserDefined.builder()
            .nullable(false)
            .urn(URN)
            .name("foo")
            .addTypeParameters(ImmutableType.ParameterDataType.builder().type(r.I8).build())
            .addTypeParameters(ImmutableType.ParameterIntegerValue.builder().value(2).build())
            .build(),
        "u!foo<i8, 2>");
  }

  private static void test(ParseToPojo.Visitor visitor, TypeExpression expected, String toParse) {
    assertEquals(expected, TypeStringParser.parse(toParse, visitor));
  }
}
