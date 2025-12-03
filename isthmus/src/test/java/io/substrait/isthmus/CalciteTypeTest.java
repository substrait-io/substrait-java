package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.function.TypeExpression;
import io.substrait.isthmus.utils.UserTypeFactory;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CalciteTypeTest extends CalciteObjs {

  // Setup for user-defined type test
  static final String uTypeURI = "/functions_custom";
  static final String uTypeName = "u_type";
  UserTypeFactory uTypeFactory = new UserTypeFactory(uTypeURI, uTypeName);

  public TypeConverter typeConverter =
      new TypeConverter(
          new UserTypeMapper() {
            @Nullable
            @Override
            public Type toSubstrait(final RelDataType relDataType) {
              if (uTypeFactory.isTypeFromFactory(relDataType)) {
                return uTypeFactory.createSubstrait(relDataType.isNullable());
              }
              return null;
            }

            @Nullable
            @Override
            public RelDataType toCalcite(final Type.UserDefined type) {
              if (type.urn().equals(uTypeURI) && type.name().equals(uTypeName)) {
                return uTypeFactory.createCalcite(type.nullable());
              }
              return null;
            }
          });

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void bool(final boolean nullable) {
    testType(Type.withNullability(nullable).BOOLEAN, SqlTypeName.BOOLEAN, nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void i8(final boolean nullable) {
    testType(Type.withNullability(nullable).I8, SqlTypeName.TINYINT, nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void i16(final boolean nullable) {
    testType(Type.withNullability(nullable).I16, SqlTypeName.SMALLINT, nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void i32(final boolean nullable) {
    testType(Type.withNullability(nullable).I32, SqlTypeName.INTEGER, nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void i64(final boolean nullable) {
    testType(Type.withNullability(nullable).I64, SqlTypeName.BIGINT, nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void fp32(final boolean nullable) {
    testType(Type.withNullability(nullable).FP32, SqlTypeName.REAL, nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void fp64(final boolean nullable) {
    testType(Type.withNullability(nullable).FP64, SqlTypeName.DOUBLE, nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void calciteFloatToFp64(final boolean nullable) {
    assertEquals(
        Type.withNullability(nullable).FP64,
        TypeConverter.DEFAULT.toSubstrait(
            type.createTypeWithNullability(type.createSqlType(SqlTypeName.FLOAT), nullable)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void date(final boolean nullable) {
    testType(Type.withNullability(nullable).DATE, SqlTypeName.DATE, nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void time(final boolean nullable) {
    testType(Type.withNullability(nullable).TIME, SqlTypeName.TIME, nullable, 6);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void precisionTimeStamp(final boolean nullable) {
    for (final int precision : new int[] {0, 3, 6}) {
      testType(
          Type.withNullability(nullable).precisionTimestamp(precision),
          SqlTypeName.TIMESTAMP,
          nullable,
          precision);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void precisionTimestamptz(final boolean nullable) {
    for (final int precision : new int[] {0, 3, 6}) {
      testType(
          Type.withNullability(nullable).precisionTimestampTZ(precision),
          SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
          nullable,
          precision);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void intervalYear(final boolean nullable) {
    testType(
        Type.withNullability(nullable).INTERVAL_YEAR,
        type.createSqlIntervalType(SubstraitTypeSystem.YEAR_MONTH_INTERVAL),
        nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void intervalDay(final boolean nullable) {
    testType(
        Type.withNullability(nullable).intervalDay(6),
        type.createSqlIntervalType(SubstraitTypeSystem.DAY_SECOND_INTERVAL),
        nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void string(final boolean nullable) {
    testType(Type.withNullability(nullable).STRING, SqlTypeName.VARCHAR, nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void binary(final boolean nullable) {
    testType(Type.withNullability(nullable).BINARY, SqlTypeName.VARBINARY, nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void fixedBinary(final boolean nullable) {
    testType(Type.withNullability(nullable).fixedBinary(74), SqlTypeName.BINARY, nullable, 74);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void fixedChar(final boolean nullable) {
    testType(Type.withNullability(nullable).fixedChar(74), SqlTypeName.CHAR, nullable, 74);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void varchar(final boolean nullable) {
    testType(Type.withNullability(nullable).varChar(74), SqlTypeName.VARCHAR, nullable, 74);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void decimal(final boolean nullable) {
    testType(Type.withNullability(nullable).decimal(38, 13), SqlTypeName.DECIMAL, nullable, 38, 13);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void list(final boolean nullable) {
    testType(
        Type.withNullability(nullable).list(TypeCreator.REQUIRED.I16),
        type.createArrayType(type.createSqlType(SqlTypeName.SMALLINT), -1),
        nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void map(final boolean nullable) {
    testType(
        Type.withNullability(nullable).map(TypeCreator.REQUIRED.STRING, TypeCreator.REQUIRED.I8),
        type.createMapType(
            type.createSqlType(SqlTypeName.VARCHAR), type.createSqlType(SqlTypeName.TINYINT)),
        nullable);
  }

  @Test
  void struct() {
    testType(
        TypeCreator.REQUIRED.struct(TypeCreator.REQUIRED.STRING, TypeCreator.REQUIRED.I8),
        type.createStructType(
            Arrays.asList(
                type.createSqlType(SqlTypeName.VARCHAR), type.createSqlType(SqlTypeName.TINYINT)),
            Arrays.asList("foo", "bar")),
        Arrays.asList("foo", "bar"));
  }

  @Test
  void nestedStruct() {
    testType(
        TypeCreator.REQUIRED.struct(
            TypeCreator.REQUIRED.struct(TypeCreator.REQUIRED.STRING, TypeCreator.REQUIRED.I8),
            TypeCreator.REQUIRED.struct(TypeCreator.REQUIRED.STRING, TypeCreator.REQUIRED.I8),
            TypeCreator.REQUIRED.STRING),
        type.createStructType(
            Arrays.asList(
                type.createStructType(
                    Arrays.asList(
                        type.createSqlType(SqlTypeName.VARCHAR),
                        type.createSqlType(SqlTypeName.TINYINT)),
                    Arrays.asList("inner1", "inner2")),
                type.createStructType(
                    Arrays.asList(
                        type.createSqlType(SqlTypeName.VARCHAR),
                        type.createSqlType(SqlTypeName.TINYINT)),
                    Arrays.asList("inner3", "inner4")),
                type.createSqlType(SqlTypeName.VARCHAR)),
            Arrays.asList("topStruct1", "topStruct2", "topVarChar")),
        Arrays.asList(
            "topStruct1", "inner1", "inner2", "topStruct2", "inner3", "inner4", "topVarChar"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void userDefinedType(final boolean nullable) {
    final Type type = uTypeFactory.createSubstrait(nullable);
    testType(typeConverter, type, uTypeFactory.createCalcite(nullable), null);
  }

  private void testType(
      final TypeExpression expression, final SqlTypeName typeName, final boolean nullable) {
    testType(expression, type.createTypeWithNullability(type.createSqlType(typeName), nullable));
  }

  private void testType(
      final TypeExpression expression,
      final SqlTypeName typeName,
      final boolean nullable,
      final int prec) {
    testType(
        expression, type.createTypeWithNullability(type.createSqlType(typeName, prec), nullable));
  }

  private void testType(
      final TypeExpression expression,
      final SqlTypeName typeName,
      final boolean nullable,
      final int prec,
      final int scale) {
    testType(
        expression,
        type.createTypeWithNullability(type.createSqlType(typeName, prec, scale), nullable));
  }

  private void testType(final TypeExpression expression, final RelDataType calciteType) {
    testType(expression, calciteType, null);
  }

  private void testType(
      final TypeExpression expression, final RelDataType calciteType, final boolean nullable) {
    testType(expression, type.createTypeWithNullability(calciteType, nullable));
  }

  private void testType(
      final TypeExpression expression,
      final RelDataType calciteType,
      final List<String> dfsFieldNames) {
    testType(TypeConverter.DEFAULT, expression, calciteType, dfsFieldNames);
  }

  private void testType(
      final TypeConverter converter,
      final TypeExpression expression,
      final RelDataType calciteType,
      final List<String> dfsFieldNames) {
    assertEquals(expression, converter.toSubstrait(calciteType));
    assertEquals(calciteType, converter.toCalcite(type, expression, dfsFieldNames));
  }
}
