package io.substrait.isthmus.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.extension.ExtensionCollector;
import io.substrait.type.TypeCreator;
import io.substrait.util.EmptyVisitationContext;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.DataContexts;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutable;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class FieldSelectionConverterTest {
  private final JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
  private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
  private final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
  private final RexExpressionConverter converter = new RexExpressionConverter();

  @ParameterizedTest
  @CsvSource({"1, 0, 10", "2, 1, 20", "3, 2, 30", "4, 3,", "0,,", "-1,,", "-2,,"})
  void itemPreservesCalciteIndexing(int index, Integer expectedOffset, Integer expectedValue) {
    RexNode call = rexBuilder.makeCall(SqlStdOperatorTable.ITEM, array(), integer(index));
    RexExecutable executable =
        RexExecutorImpl.getExecutable(rexBuilder, List.of(call), typeFactory.builder().build());
    executable.setDataContext(DataContexts.EMPTY);
    assertEquals(expectedValue, executable.execute()[0]);

    Expression converted = call.accept(converter);
    if (expectedOffset == null) {
      assertEquals(
          TypeCreator.NULLABLE.I32,
          assertInstanceOf(Expression.NullLiteral.class, converted).getType());
    } else {
      assertEquals(expectedOffset.intValue(), listOffset(converted));
    }
  }

  @ParameterizedTest
  @MethodSource("safeIndexing")
  void safeOperatorsUseTheirOwnBase(SqlOperator operator, long index, int expectedOffset) {
    Expression expression =
        rexBuilder.makeCall(operator, array(), integer(index)).accept(converter);
    assertEquals(expectedOffset, listOffset(expression));

    // The same offset is used when dereferencing an array column rather than an expression.
    RexNode column = rexBuilder.makeInputRef(array().getType(), 0);
    Expression columnExpression =
        rexBuilder.makeCall(operator, column, integer(index)).accept(converter);
    io.substrait.proto.Expression proto = toProto(columnExpression);
    assertEquals(
        expectedOffset,
        proto
            .getSelection()
            .getDirectReference()
            .getStructField()
            .getChild()
            .getListElement()
            .getOffset());
  }

  private static Stream<Arguments> safeIndexing() {
    return Stream.of(
        Arguments.of(SqlStdOperatorTable.ITEM, 1L, 0),
        Arguments.of(SqlStdOperatorTable.ITEM, 2147483648L, Integer.MAX_VALUE),
        Arguments.of(SqlLibraryOperators.SAFE_OFFSET, 0L, 0),
        Arguments.of(SqlLibraryOperators.SAFE_OFFSET, 1L, 1),
        Arguments.of(SqlLibraryOperators.SAFE_OFFSET, 3L, 3),
        Arguments.of(SqlLibraryOperators.SAFE_OFFSET, (long) Integer.MAX_VALUE, Integer.MAX_VALUE),
        Arguments.of(SqlLibraryOperators.SAFE_ORDINAL, 1L, 0),
        Arguments.of(SqlLibraryOperators.SAFE_ORDINAL, 3L, 2),
        Arguments.of(SqlLibraryOperators.SAFE_ORDINAL, 2147483648L, Integer.MAX_VALUE));
  }

  @ParameterizedTest
  @MethodSource("invalidLowIndexes")
  void invalidLowIndexesAreNull(SqlOperator operator, long index) {
    Expression converted = rexBuilder.makeCall(operator, array(), integer(index)).accept(converter);
    assertEquals(
        TypeCreator.NULLABLE.I32,
        assertInstanceOf(Expression.NullLiteral.class, converted).getType());
  }

  private static Stream<Arguments> invalidLowIndexes() {
    return Stream.of(
        Arguments.of(SqlStdOperatorTable.ITEM, 0L),
        Arguments.of(SqlStdOperatorTable.ITEM, Long.MIN_VALUE),
        Arguments.of(SqlLibraryOperators.SAFE_OFFSET, -1L),
        Arguments.of(SqlLibraryOperators.SAFE_OFFSET, Long.MIN_VALUE),
        Arguments.of(SqlLibraryOperators.SAFE_ORDINAL, 0L),
        Arguments.of(SqlLibraryOperators.SAFE_ORDINAL, -1L));
  }

  @Test
  void nullIndexIsNull() {
    Expression converted =
        rexBuilder
            .makeCall(SqlStdOperatorTable.ITEM, array(), rexBuilder.makeNullLiteral(intType))
            .accept(converter);
    assertEquals(
        TypeCreator.NULLABLE.I32,
        assertInstanceOf(Expression.NullLiteral.class, converted).getType());
  }

  @ParameterizedTest
  @MethodSource("nullOrInvalidIndexes")
  void rejectsDiscardingThrowingArray(SqlOperator operator, Integer index) {
    RexNode cast =
        rexBuilder.makeAbstractCast(intType, rexBuilder.makeLiteral("not an integer"), false);
    RexNode throwingArray = rexBuilder.makeCall(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, cast);
    RexNode nestedArray =
        rexBuilder.makeCall(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, throwingArray);
    RexNode nestedSelection =
        rexBuilder.makeCall(SqlStdOperatorTable.ITEM, nestedArray, integer(1));

    for (RexNode input : List.of(throwingArray, nestedSelection)) {
      RexNode call = rexBuilder.makeCall(operator, input, nullableIndex(index));
      RexExecutable executable =
          RexExecutorImpl.getExecutable(rexBuilder, List.of(call), typeFactory.builder().build());
      executable.setDataContext(DataContexts.EMPTY);
      assertThrows(NumberFormatException.class, executable::execute);
      assertThrows(IllegalArgumentException.class, () -> call.accept(converter));
    }
  }

  @ParameterizedTest
  @MethodSource("nullOrInvalidIndexes")
  void rejectsDiscardingNonliteralArray(SqlOperator operator, Integer index) {
    RexNode text = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR, 20), 0);
    RexNode cast = rexBuilder.makeAbstractCast(intType, text, false);
    RexNode array = rexBuilder.makeCall(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, cast);
    RexNode call = rexBuilder.makeCall(operator, array, nullableIndex(index));

    assertThrows(IllegalArgumentException.class, () -> call.accept(converter));
  }

  @ParameterizedTest
  @MethodSource("nullOrInvalidIndexes")
  void literalAndColumnArraysCanBeDiscarded(SqlOperator operator, Integer index) {
    RexNode column = rexBuilder.makeInputRef(array().getType(), 0);
    for (RexNode input : List.of(array(), column)) {
      Expression converted =
          rexBuilder.makeCall(operator, input, nullableIndex(index)).accept(converter);
      assertEquals(
          TypeCreator.NULLABLE.I32,
          assertInstanceOf(Expression.NullLiteral.class, converted).getType());
    }
  }

  private static Stream<Arguments> nullOrInvalidIndexes() {
    return Stream.of(
        Arguments.of(SqlStdOperatorTable.ITEM, 0),
        Arguments.of(SqlStdOperatorTable.ITEM, -1),
        Arguments.of(SqlStdOperatorTable.ITEM, null),
        Arguments.of(SqlLibraryOperators.SAFE_OFFSET, -1),
        Arguments.of(SqlLibraryOperators.SAFE_OFFSET, null),
        Arguments.of(SqlLibraryOperators.SAFE_ORDINAL, 0),
        Arguments.of(SqlLibraryOperators.SAFE_ORDINAL, null));
  }

  private RexNode nullableIndex(Integer index) {
    return index == null ? rexBuilder.makeNullLiteral(intType) : integer(index);
  }

  @ParameterizedTest
  @ValueSource(longs = {2147483649L, 4294967297L, Long.MAX_VALUE})
  void unrepresentableOffsetsAreRejected(long index) {
    RexNode call = rexBuilder.makeCall(SqlStdOperatorTable.ITEM, array(), integer(index));
    assertThrows(IllegalArgumentException.class, () -> call.accept(converter));
  }

  @ParameterizedTest
  @MethodSource("unsafeOperators")
  void throwingOperatorsAreRejected(SqlOperator operator) {
    RexNode call = rexBuilder.makeCall(operator, array(), integer(4));
    assertThrows(IllegalArgumentException.class, () -> call.accept(converter));
  }

  private static Stream<SqlOperator> unsafeOperators() {
    return Stream.of(SqlLibraryOperators.OFFSET, SqlLibraryOperators.ORDINAL);
  }

  private RexNode array() {
    return rexBuilder.makeCall(
        SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, integer(10), integer(20), integer(30));
  }

  private RexNode integer(long value) {
    RelDataType type =
        value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE
            ? intType
            : typeFactory.createSqlType(SqlTypeName.BIGINT);
    return rexBuilder.makeExactLiteral(BigDecimal.valueOf(value), type);
  }

  private int listOffset(Expression expression) {
    assertInstanceOf(FieldReference.class, expression);
    return toProto(expression).getSelection().getDirectReference().getListElement().getOffset();
  }

  private io.substrait.proto.Expression toProto(Expression expression) {
    return expression.accept(
        new ExpressionProtoConverter(new ExtensionCollector(), null),
        EmptyVisitationContext.INSTANCE);
  }
}
