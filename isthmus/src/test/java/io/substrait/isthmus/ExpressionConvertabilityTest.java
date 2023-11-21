package io.substrait.isthmus;

import static io.substrait.isthmus.expression.CallConverters.CASE;
import static io.substrait.isthmus.expression.CallConverters.CREATE_SEARCH_CONV;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.extension.ExtensionCollector;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

/** Tests which test that an expression can be converted to and from Calcite expressions. */
public class ExpressionConvertabilityTest extends PlanTestBase {

  static final TypeCreator R = TypeCreator.of(false);
  static final TypeCreator N = TypeCreator.of(true);

  final SubstraitBuilder b = new SubstraitBuilder(extensions);

  final ExpressionProtoConverter expressionProtoConverter =
      new ExpressionProtoConverter(new ExtensionCollector(), null);

  final ExpressionRexConverter converter =
      new ExpressionRexConverter(
          typeFactory,
          new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory),
          new WindowFunctionConverter(extensions.windowFunctions(), typeFactory),
          TypeConverter.DEFAULT);

  final RexBuilder rexBuilder = new RexBuilder(typeFactory);

  // Define a shared table (i.e. a NamedScan) for use in tests.
  final List<Type> commonTableType = List.of(R.I32, R.FP32, N.STRING, N.BOOLEAN);
  final Rel commonTable =
      b.namedScan(List.of("example"), List.of("a", "b", "c", "d"), commonTableType);

  @Test
  public void listLiteral() throws IOException, SqlParseException {
    assertFullRoundTrip("select ARRAY[1,2,3] from ORDERS");
  }

  @Test
  public void mapLiteral() throws IOException, SqlParseException {
    assertFullRoundTrip("select MAP[1, 'hello'] from ORDERS");
  }

  @Test
  public void inPredicate() throws IOException, SqlParseException {
    assertFullRoundTrip(
        "select L_PARTKEY from LINEITEM where L_PARTKEY in "
            + "(SELECT L_SUPPKEY from LINEITEM where L_SUPPKEY < L_ORDERKEY)");
  }

  @Test
  public void singleOrList() {
    Expression singleOrList = b.singleOrList(b.fieldReference(commonTable, 0), b.i32(5), b.i32(10));
    RexNode rexNode = singleOrList.accept(converter);
    Expression substraitExpression =
        rexNode.accept(
            new RexExpressionConverter(
                CREATE_SEARCH_CONV.apply(rexBuilder),
                new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory)));

    // cannot roundtrip test singleOrList because Calcite simplifies the representation
    assertExpressionEquality(
        b.or(
            b.equal(b.fieldReference(commonTable, 0), b.i32(5)),
            b.equal(b.fieldReference(commonTable, 0), b.i32(10))),
        substraitExpression);
  }

  @Test
  public void switchExpression() {
    Expression switchExpression =
        b.switchExpression(
            b.fieldReference(commonTable, 0),
            List.of(b.switchClause(b.i32(5), b.i32(1)), b.switchClause(b.i32(10), b.i32(2))),
            b.i32(3));
    RexNode rexNode = switchExpression.accept(converter);
    Expression expression =
        rexNode.accept(
            new RexExpressionConverter(
                CASE, new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory)));

    // cannot roundtrip test switchExpression because Calcite simplifies the representation
    assertExpressionEquality(
        b.ifThen(
            List.of(
                b.ifClause(b.equal(b.fieldReference(commonTable, 0), b.i32(5)), b.i32(1)),
                b.ifClause(b.equal(b.fieldReference(commonTable, 0), b.i32(10)), b.i32(2))),
            b.i32(3)),
        expression);
  }

  void assertExpressionEquality(Expression expected, Expression actual) {
    // go the extra mile and convert both inputs to protobuf
    // helps verify that the protobuf conversion is not broken
    assertEquals(
        expected.accept(expressionProtoConverter), actual.accept(expressionProtoConverter));
  }
}
