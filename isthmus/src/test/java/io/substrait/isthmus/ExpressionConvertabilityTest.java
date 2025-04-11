package io.substrait.isthmus;

import static io.substrait.isthmus.expression.CallConverters.CASE;
import static io.substrait.isthmus.expression.CallConverters.CREATE_SEARCH_CONV;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
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
import org.apache.calcite.rex.RexLiteral;
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

  @Test
  public void castFailureCondition() {
    Rel rel =
        b.project(
            input ->
                List.of(
                    ExpressionCreator.cast(
                        R.I64,
                        b.fieldReference(input, 0),
                        Expression.FailureBehavior.THROW_EXCEPTION),
                    ExpressionCreator.cast(
                        R.I32, b.fieldReference(input, 0), Expression.FailureBehavior.RETURN_NULL)),
            b.remap(1, 2),
            b.namedScan(List.of("test"), List.of("col1"), List.of(R.STRING)));

    assertFullRoundTrip(rel);
  }

  void assertExpressionEquality(Expression expected, Expression actual) {
    // go the extra mile and convert both inputs to protobuf
    // helps verify that the protobuf conversion is not broken
    assertEquals(
        expected.accept(expressionProtoConverter), actual.accept(expressionProtoConverter));
  }

  @Test
  public void supportedPrecisionForPrecisionTimestampLiteral() {
    assertPrecisionTimestampLiteral(0);
    assertPrecisionTimestampLiteral(3);
    assertPrecisionTimestampLiteral(6);
  }

  void assertPrecisionTimestampLiteral(int precision) {
    RexNode calciteExpr =
        Expression.PrecisionTimestampLiteral.builder()
            .value(0)
            .precision(precision)
            .build()
            .accept(converter);
    assertInstanceOf(RexLiteral.class, calciteExpr);
  }

  @Test
  public void supportedPrecisionForPrecisionTimestampTZLiteral() {
    assertPrecisionTimestampTZLiteral(0);
    assertPrecisionTimestampTZLiteral(3);
    assertPrecisionTimestampTZLiteral(6);
  }

  void assertPrecisionTimestampTZLiteral(int precision) {
    RexNode calciteExpr =
        Expression.PrecisionTimestampTZLiteral.builder()
            .value(0)
            .precision(precision)
            .build()
            .accept(converter);
    assertInstanceOf(RexLiteral.class, calciteExpr);
  }

  @Test
  public void unsupportedPrecisionForPrecisionTimestampLiteral() {
    // test different edge case precision values
    assertThrowsUnsupportedPrecisionPrecisionTimestampLiteral(-1);
    assertThrowsUnsupportedPrecisionPrecisionTimestampLiteral(1);
    assertThrowsUnsupportedPrecisionPrecisionTimestampLiteral(2);

    assertThrowsUnsupportedPrecisionPrecisionTimestampLiteral(4);
    assertThrowsUnsupportedPrecisionPrecisionTimestampLiteral(5);

    assertThrowsUnsupportedPrecisionPrecisionTimestampLiteral(7);
    assertThrowsUnsupportedPrecisionPrecisionTimestampLiteral(8);

    // this would be nanoseconds which are supported in Substrait but not in Calcite
    assertThrowsUnsupportedPrecisionPrecisionTimestampLiteral(9);

    assertThrowsUnsupportedPrecisionPrecisionTimestampLiteral(10);
    assertThrowsUnsupportedPrecisionPrecisionTimestampLiteral(11);

    // this would be picoseconds which are supported in Substrait but not in Calcite
    assertThrowsUnsupportedPrecisionPrecisionTimestampLiteral(12);

    // everything above 12 is neither supported in Substrait nor in Calcite
    assertThrowsUnsupportedPrecisionPrecisionTimestampLiteral(13);
  }

  void assertThrowsUnsupportedPrecisionPrecisionTimestampLiteral(int precision) {
    assertThrowsExpressionLiteral(
        Expression.PrecisionTimestampLiteral.builder().value(0).precision(precision).build());
  }

  @Test
  public void unsupportedPrecisionPrecisionTimestampTZLiteral() {
    // test different edge case precision values
    assertThrowsUnsupportedPrecisionPrecisionTimestampTZLiteral(-1);
    assertThrowsUnsupportedPrecisionPrecisionTimestampTZLiteral(1);
    assertThrowsUnsupportedPrecisionPrecisionTimestampTZLiteral(2);

    assertThrowsUnsupportedPrecisionPrecisionTimestampTZLiteral(4);
    assertThrowsUnsupportedPrecisionPrecisionTimestampTZLiteral(5);

    assertThrowsUnsupportedPrecisionPrecisionTimestampTZLiteral(7);
    assertThrowsUnsupportedPrecisionPrecisionTimestampTZLiteral(8);

    // this would be nanoseconds which are supported in Substrait but not in Calcite
    assertThrowsUnsupportedPrecisionPrecisionTimestampTZLiteral(9);

    assertThrowsUnsupportedPrecisionPrecisionTimestampTZLiteral(10);
    assertThrowsUnsupportedPrecisionPrecisionTimestampTZLiteral(11);

    // this would be picoseconds which are supported in Substrait but not in Calcite
    assertThrowsUnsupportedPrecisionPrecisionTimestampTZLiteral(12);

    // everything above 12 is neither supported in Substrait nor in Calcite
    assertThrowsUnsupportedPrecisionPrecisionTimestampTZLiteral(13);
  }

  void assertThrowsUnsupportedPrecisionPrecisionTimestampTZLiteral(int precision) {
    assertThrowsExpressionLiteral(
        Expression.PrecisionTimestampTZLiteral.builder().value(0).precision(precision).build());
  }

  void assertThrowsExpressionLiteral(Expression.Literal expr) {
    assertThrows(UnsupportedOperationException.class, () -> expr.accept(converter));
  }
}
