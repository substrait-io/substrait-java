package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.function.TypeExpression;
import io.substrait.isthmus.utils.UserTypeFactory;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CalciteTypeTest extends CalciteObjs {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CalciteTypeTest.class);

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void bool(boolean nullable) {
    testType(Type.withNullability(nullable).BOOLEAN, SqlTypeName.BOOLEAN, nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void i8(boolean nullable) {
    testType(Type.withNullability(nullable).I8, SqlTypeName.TINYINT, nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void i16(boolean nullable) {
    testType(Type.withNullability(nullable).I16, SqlTypeName.SMALLINT, nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void i32(boolean nullable) {
    testType(Type.withNullability(nullable).I32, SqlTypeName.INTEGER, nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void i64(boolean nullable) {
    testType(Type.withNullability(nullable).I64, SqlTypeName.BIGINT, nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void fp32(boolean nullable) {
    testType(Type.withNullability(nullable).FP32, SqlTypeName.REAL, nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void fp64(boolean nullable) {
    testType(Type.withNullability(nullable).FP64, SqlTypeName.DOUBLE, nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void date(boolean nullable) {
    testType(Type.withNullability(nullable).DATE, SqlTypeName.DATE, nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void time(boolean nullable) {
    testType(Type.withNullability(nullable).TIME, SqlTypeName.TIME, nullable, 6);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void timestamp(boolean nullable) {
    testType(Type.withNullability(nullable).TIMESTAMP, SqlTypeName.TIMESTAMP, nullable, 6);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void timestamptz(boolean nullable) {
    testType(
        Type.withNullability(nullable).TIMESTAMP_TZ,
        SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        nullable,
        6);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void intervalYear(boolean nullable) {
    testType(
        Type.withNullability(nullable).INTERVAL_YEAR,
        type.createSqlIntervalType(SubstraitTypeSystem.YEAR_MONTH_INTERVAL),
        nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void intervalDay(boolean nullable) {
    testType(
        Type.withNullability(nullable).INTERVAL_DAY,
        type.createSqlIntervalType(SubstraitTypeSystem.DAY_SECOND_INTERVAL),
        nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void string(boolean nullable) {
    testType(Type.withNullability(nullable).STRING, SqlTypeName.VARCHAR, nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void binary(boolean nullable) {
    testType(Type.withNullability(nullable).BINARY, SqlTypeName.VARBINARY, nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void fixedBinary(boolean nullable) {
    testType(Type.withNullability(nullable).fixedBinary(74), SqlTypeName.BINARY, nullable, 74);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void fixedChar(boolean nullable) {
    testType(Type.withNullability(nullable).fixedChar(74), SqlTypeName.CHAR, nullable, 74);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void varchar(boolean nullable) {
    testType(Type.withNullability(nullable).varChar(74), SqlTypeName.VARCHAR, nullable, 74);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void decimal(boolean nullable) {
    testType(Type.withNullability(nullable).decimal(38, 13), SqlTypeName.DECIMAL, nullable, 38, 13);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void list(boolean nullable) {
    testType(
        Type.withNullability(nullable).list(TypeCreator.REQUIRED.I16),
        type.createArrayType(type.createSqlType(SqlTypeName.SMALLINT), -1),
        nullable);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void map(boolean nullable) {
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

  // Setup for user-defined type test
  static final String uTypeURI = "/functions_custom";
  static final String uTypeName = "u_type";
  UserTypeFactory uTypeFactory = new UserTypeFactory(uTypeURI, uTypeName);

  public TypeConverter typeConverter =
      new TypeConverter(
          new UserTypeMapper() {
            @Nullable
            @Override
            public Type toSubstrait(RelDataType relDataType) {
              if (uTypeFactory.isTypeFromFactory(relDataType)) {
                return uTypeFactory.createSubstrait(relDataType.isNullable());
              }
              return null;
            }

            @Nullable
            @Override
            public RelDataType toCalcite(Type.UserDefined type) {
              if (type.uri().equals(uTypeURI) && type.name().equals(uTypeName)) {
                return uTypeFactory.createCalcite(type.nullable());
              }
              return null;
            }
          });

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void userDefinedType(boolean nullable) {
    var type = uTypeFactory.createSubstrait(nullable);
    testType(typeConverter, type, uTypeFactory.createCalcite(nullable), null);
  }

  private void testType(TypeExpression expression, SqlTypeName typeName, boolean nullable) {
    testType(expression, type.createTypeWithNullability(type.createSqlType(typeName), nullable));
  }

  private void testType(
      TypeExpression expression, SqlTypeName typeName, boolean nullable, int prec) {
    testType(
        expression, type.createTypeWithNullability(type.createSqlType(typeName, prec), nullable));
  }

  private void testType(
      TypeExpression expression, SqlTypeName typeName, boolean nullable, int prec, int scale) {
    testType(
        expression,
        type.createTypeWithNullability(type.createSqlType(typeName, prec, scale), nullable));
  }

  private void testType(TypeExpression expression, RelDataType calciteType) {
    testType(expression, calciteType, null);
  }

  private void testType(TypeExpression expression, RelDataType calciteType, boolean nullable) {
    testType(expression, type.createTypeWithNullability(calciteType, nullable));
  }

  private void testType(
      TypeExpression expression, RelDataType calciteType, List<String> dfsFieldNames) {
    testType(TypeConverter.DEFAULT, expression, calciteType, dfsFieldNames);
  }

  private void testType(
      TypeConverter converter,
      TypeExpression expression,
      RelDataType calciteType,
      List<String> dfsFieldNames) {
    assertEquals(expression, converter.toSubstrait(calciteType));
    assertEquals(calciteType, converter.toCalcite(type, expression, dfsFieldNames));
  }
}
