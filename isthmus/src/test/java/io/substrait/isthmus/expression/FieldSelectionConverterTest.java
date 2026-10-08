package io.substrait.isthmus.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.expression.proto.ProtoExpressionConverter;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.ExtensionCollector;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.UserTypeMapper;
import io.substrait.isthmus.utils.UserTypeFactory;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.util.EmptyVisitationContext;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.calcite.DataContexts;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
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
    assertEquals(
        expectedOffset == null ? Integer.MAX_VALUE : expectedOffset, listOffset(converted));
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
        listOffset(proto.getSelection().getDirectReference().getStructField().getChild()));
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
    assertEquals(Integer.MAX_VALUE, listOffset(converted));
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
    assertEquals(Integer.MAX_VALUE, listOffset(converted));
  }

  @Test
  void untypedNullIndexIsNull() {
    Expression converted =
        rexBuilder
            .makeCall(
                SqlStdOperatorTable.ITEM,
                array(),
                rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.NULL)))
            .accept(converter);
    assertEquals(Integer.MAX_VALUE, listOffset(converted));
    verifyRoundTrip(converted, TypeConverter.DEFAULT.toSubstrait(array().getType()));
  }

  @ParameterizedTest
  @MethodSource("nullOrInvalidIndexes")
  void retainsThrowingArray(SqlOperator operator, Integer index) {
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
      FieldReference converted = assertInstanceOf(FieldReference.class, call.accept(converter));
      assertEquals(
          Integer.MAX_VALUE, ((FieldReference.ListElement) converted.segments().get(0)).offset());
      Expression source = input.accept(converter);
      assertEquals(
          source instanceof FieldReference
              ? ((FieldReference) source).inputExpression().orElseThrow()
              : source,
          converted.inputExpression().orElseThrow());
    }
  }

  @ParameterizedTest
  @MethodSource("nullOrInvalidIndexes")
  void retainsNonliteralArray(SqlOperator operator, Integer index) {
    RexNode text = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR, 20), 0);
    RexNode cast = rexBuilder.makeAbstractCast(intType, text, false);
    RexNode array = rexBuilder.makeCall(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, cast);
    RexNode call = rexBuilder.makeCall(operator, array, nullableIndex(index));

    FieldReference converted = assertInstanceOf(FieldReference.class, call.accept(converter));
    assertEquals(Integer.MAX_VALUE, listOffset(converted));
    assertEquals(array.accept(converter), converted.inputExpression().orElseThrow());
  }

  @ParameterizedTest
  @MethodSource("nullOrInvalidIndexes")
  void literalAndColumnArraysRetainReferences(SqlOperator operator, Integer index) {
    RexNode column = rexBuilder.makeInputRef(array().getType(), 0);
    for (RexNode input : List.of(array(), column)) {
      Expression converted =
          rexBuilder.makeCall(operator, input, nullableIndex(index)).accept(converter);
      FieldReference reference = assertInstanceOf(FieldReference.class, converted);
      assertEquals(
          Integer.MAX_VALUE, ((FieldReference.ListElement) reference.segments().get(0)).offset());
      verifyRoundTrip(converted, TypeConverter.DEFAULT.toSubstrait(input.getType()));
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

  @Test
  void invalidIndexesRemainReferencesInsideCollections() {
    RexNode column = rexBuilder.makeInputRef(array().getType(), 0);
    RexNode missing = rexBuilder.makeCall(SqlStdOperatorTable.ITEM, column, integer(0));
    Expression list =
        rexBuilder
            .makeCall(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, integer(1), missing)
            .accept(converter);
    assertInstanceOf(Expression.NestedList.class, list);
    verifyRoundTrip(list, TypeConverter.DEFAULT.toSubstrait(column.getType()));
    Expression map =
        rexBuilder
            .makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, missing, rexBuilder.makeLiteral("x"))
            .accept(converter);
    assertInstanceOf(Expression.NestedMap.class, map);
    verifyRoundTrip(map, TypeConverter.DEFAULT.toSubstrait(column.getType()));

    RelDataType nestedType =
        typeFactory.createArrayType(typeFactory.createArrayType(array().getType(), -1), -1);
    RexNode nested = rexBuilder.makeInputRef(nestedType, 0);
    for (int index : new int[] {0, 1, 0}) {
      nested = rexBuilder.makeCall(SqlStdOperatorTable.ITEM, nested, integer(index));
    }
    FieldReference reference = assertInstanceOf(FieldReference.class, nested.accept(converter));
    assertEquals(4, reference.segments().size());
    verifyRoundTrip(reference, TypeConverter.DEFAULT.toSubstrait(nestedType));
  }

  @Test
  void rowElementTypesRemainIdenticalForValidAndInvalidIndexes() {
    RelDataType row = typeFactory.builder().add("x", intType).build();
    RexNode column = rexBuilder.makeInputRef(typeFactory.createArrayType(row, -1), 0);
    RexNode first = rexBuilder.makeCall(SqlStdOperatorTable.ITEM, column, integer(1));
    RexNode missing = rexBuilder.makeCall(SqlStdOperatorTable.ITEM, column, integer(0));
    Expression list =
        rexBuilder
            .makeCall(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, missing, first)
            .accept(converter);
    assertEquals(first.accept(converter).getType(), missing.accept(converter).getType());
    assertInstanceOf(Expression.NestedList.class, list);
    verifyRoundTrip(list, TypeConverter.DEFAULT.toSubstrait(column.getType()));
  }

  @Test
  void requiredUserDefinedElementsCanBeAccessedWithInvalidIndexes() {
    UserTypeFactory factory = new UserTypeFactory("extension:test:array", "element");
    TypeConverter types =
        new TypeConverter(
            new UserTypeMapper() {
              @Override
              public Type toSubstrait(RelDataType type) {
                return factory.isTypeFromFactory(type)
                    ? factory.createSubstrait(type.isNullable(), factory.getTypeParameters(type))
                    : null;
              }

              @Override
              public RelDataType toCalcite(Type.UserDefined type) {
                return factory.createCalcite(type.nullable(), type.typeParameters());
              }
            });
    RexExpressionConverter custom =
        new RexExpressionConverter(null, CallConverters.defaults(types), null, types);
    RexNode column =
        rexBuilder.makeInputRef(typeFactory.createArrayType(factory.createCalcite(false), -1), 0);
    for (Integer index : new Integer[] {0, null}) {
      Expression converted =
          rexBuilder
              .makeCall(SqlStdOperatorTable.ITEM, column, nullableIndex(index))
              .accept(custom);
      assertInstanceOf(FieldReference.class, converted);
      verifyRoundTrip(converted, types.toSubstrait(column.getType()));
    }
  }

  @ParameterizedTest
  @ValueSource(longs = {2147483649L, 4294967297L, Long.MAX_VALUE})
  void offsetsPastTheListLengthLimitAreNull(long index) {
    RexNode call = rexBuilder.makeCall(SqlStdOperatorTable.ITEM, array(), integer(index));
    Expression converted = call.accept(converter);
    assertEquals(Integer.MAX_VALUE, listOffset(converted));
  }

  @ParameterizedTest
  @MethodSource("unsafeOperators")
  void throwingOperatorsAreRejected(SqlOperator operator) {
    RexNode call = rexBuilder.makeCall(operator, array(), integer(4));
    assertThrows(IllegalArgumentException.class, () -> call.accept(converter));
  }

  @ParameterizedTest
  @MethodSource("unsafeOperators")
  void declinedOperatorsDoNotConvertTheArrayOperand(SqlOperator operator) {
    RexNode call = rexBuilder.makeCall(operator, array(), integer(1));
    assertEquals(
        Optional.empty(),
        new FieldSelectionConverter(TypeConverter.DEFAULT)
            .convert(
                (RexCall) call,
                operand -> {
                  throw new AssertionError("converted " + operand);
                }));
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
    return listOffset(toProto(expression).getSelection().getDirectReference());
  }

  private int listOffset(io.substrait.proto.Expression.ReferenceSegment segment) {
    // Protobuf returns offset 0 for a segment that is not a list element.
    assertTrue(segment.hasListElement());
    return segment.getListElement().getOffset();
  }

  private io.substrait.proto.Expression toProto(Expression expression) {
    return expression.accept(
        new ExpressionProtoConverter(new ExtensionCollector(), null),
        EmptyVisitationContext.INSTANCE);
  }

  private void verifyRoundTrip(Expression expression, Type inputType) {
    ExtensionCollector extensions = new ExtensionCollector();
    io.substrait.proto.Expression proto =
        new ExpressionProtoConverter(extensions, null).toProto(expression);
    Expression restored =
        new ProtoExpressionConverter(
                extensions,
                DefaultExtensionCatalog.DEFAULT_COLLECTION,
                TypeCreator.REQUIRED.struct(inputType),
                null)
            .from(proto);
    assertEquals(expression, restored);
  }
}
