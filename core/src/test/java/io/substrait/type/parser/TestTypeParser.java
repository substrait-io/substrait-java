package io.substrait.type.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.function.ParameterizedTypeCreator;
import io.substrait.function.TypeExpression;
import io.substrait.function.TypeExpressionCreator;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.junit.jupiter.api.Test;

public class TestTypeParser {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestTypeParser.class);

  private final TypeCreator n = Type.NULLABLE;
  private final TypeCreator r = Type.REQUIRED;

  private final TypeExpressionCreator eo = TypeExpressionCreator.REQUIRED;
  private final TypeExpressionCreator en = TypeExpressionCreator.NULLABLE;
  private final ParameterizedTypeCreator pr = ParameterizedTypeCreator.REQUIRED;
  private final ParameterizedTypeCreator pn = ParameterizedTypeCreator.NULLABLE;

  @Test
  public void basic() {
    simpleTests(ParseToPojo.Visitor.SIMPLE);
  }

  @Test
  public void compound() {
    compoundTests(ParseToPojo.Visitor.SIMPLE);
  }

  @Test
  public void parameterizedSimple() {
    simpleTests(ParseToPojo.Visitor.PARAMETERIZED);
  }

  @Test
  public void parameterizedCompound() {
    compoundTests(ParseToPojo.Visitor.PARAMETERIZED);
  }

  @Test
  public void parameterizedParameterized() {
    parameterizedTests(ParseToPojo.Visitor.PARAMETERIZED);
  }

  @Test
  public void derivationSimple() {
    simpleTests(ParseToPojo.Visitor.EXPRESSION);
  }

  @Test
  public void derivationCompound() {
    compoundTests(ParseToPojo.Visitor.EXPRESSION);
  }

  @Test
  public void derivationParameterized() {
    parameterizedTests(ParseToPojo.Visitor.EXPRESSION);
  }

  @Test
  public void derivationExpression() {
    test(
        ParseToPojo.Visitor.EXPRESSION,
        eo.fixedCharE(eo.plus(pr.parameter("L1"), pr.parameter("L2"))),
        "FIXEDCHAR<L1+L2>");
    test(
        ParseToPojo.Visitor.EXPRESSION,
        eo.program(pr.fixedCharE("L1"), new TypeExpressionCreator.Assign("L1", eo.i(1))),
        "L1=1\nFIXEDCHAR<L1>");
  }

  private <T> void simpleTests(ParseToPojo.Visitor v) {
    test(v, r.I8, "I8");
    test(v, r.I16, "I16");
    test(v, r.I32, "I32");
    test(v, r.I64, "I64");
    test(v, r.FP32, "FP32");
    test(v, r.FP64, "FP64");

    // Nullable
    test(v, n.I8, "I8?");
    test(v, n.I16, "I16?");
    test(v, n.I32, "i32?");
    test(v, n.I64, "i64?");
    test(v, n.FP32, "FP32?");
    test(v, n.FP64, "FP64?");
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
  }

  private <T> void parameterizedTests(ParseToPojo.Visitor v) {
    test(v, pn.listE(pr.parameter("K")), "List?<K>");
    test(v, pr.structE(r.I8, r.I16, n.I8, pr.parameter("K")), "STRUCT<i8, i16, i8?, K>");
    test(v, pn.parameter("any"), "any");
    test(v, pn.listE(pr.parameter("any")), "list?<any>");
    test(v, pn.listE(pn.parameter("any")), "list?<any?>");
    test(v, pn.structE(r.I8, r.I16, n.I8, pr.parameter("K")), "STRUCT?<i8, i16, i8?, K>");
  }

  private static void test(ParseToPojo.Visitor visitor, TypeExpression expected, String toParse) {
    assertEquals(expected, TypeStringParser.parse(toParse, visitor));
  }
}
