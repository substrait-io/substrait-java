package io.substrait.isthmus;

import static io.substrait.isthmus.SubstraitTypeSystem.YEAR_MONTH_INTERVAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import io.substrait.expression.Expression;
import io.substrait.expression.Expression.IntervalDayLiteral;
import io.substrait.expression.Expression.IntervalYearLiteral;
import io.substrait.expression.Expression.Literal;
import io.substrait.expression.Expression.PrecisionTimestampLiteral;
import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.SubstraitRelNodeConverter.Context;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.LiteralConverter;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.type.TypeCreator;
import io.substrait.util.DecimalUtil;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class CalciteLiteralTest extends CalciteObjs {
  protected static final SimpleExtension.ExtensionCollection EXTENSION_COLLECTION =
      DefaultExtensionCatalog.DEFAULT_COLLECTION;

  private final ScalarFunctionConverter scalarFunctionConverter =
      new ScalarFunctionConverter(EXTENSION_COLLECTION.scalarFunctions(), type);

  private final ExpressionRexConverter expressionRexConverter =
      new ExpressionRexConverter(type, scalarFunctionConverter, null, TypeConverter.DEFAULT);

  private final RexExpressionConverter rexExpressionConverter = new RexExpressionConverter();

  @Test
  void nullLiteral() {
    bitest(
        ExpressionCreator.typedNull(TypeCreator.NULLABLE.varChar(10)),
        rex.makeNullLiteral(tN(SqlTypeName.VARCHAR, 10)));
  }

  @Test
  void tI8() {
    bitest(ExpressionCreator.i8(false, 4), c(4, SqlTypeName.TINYINT));
  }

  @Test
  void tI16() {
    bitest(ExpressionCreator.i16(false, 4), c(4, SqlTypeName.SMALLINT));
  }

  @Test
  void tI32() {
    bitest(ExpressionCreator.i32(false, 4), c(4, SqlTypeName.INTEGER));
  }

  @Test
  void tI64() {
    bitest(ExpressionCreator.i64(false, 1234L), c(1234L, SqlTypeName.BIGINT));
  }

  @Test
  void tFP32() {
    bitest(ExpressionCreator.fp32(false, 4.44F), c(4.44F, SqlTypeName.REAL));
  }

  @Test
  void tFP64() {
    bitest(ExpressionCreator.fp64(false, 4.45F), c(4.45F, SqlTypeName.DOUBLE));
  }

  @Test
  void tFloatFP64() {
    test(ExpressionCreator.fp64(false, 4.45F), c(4.45F, SqlTypeName.FLOAT));
  }

  @Test
  void tStr() {
    bitest(ExpressionCreator.string(false, "my test"), c("my test", SqlTypeName.VARCHAR));
  }

  @Test
  void tBinary() {
    byte[] val = "my test".getBytes(StandardCharsets.UTF_8);
    bitest(
        ExpressionCreator.binary(false, val),
        c(new org.apache.calcite.avatica.util.ByteString(val), SqlTypeName.VARBINARY));
  }

  @Test
  void tTime() {
    bitest(
        ExpressionCreator.precisionTime(false, (14L * 60 * 60 + 22 * 60 + 47) * 1000 * 1000, 6),
        rex.makeTimeLiteral(new TimeString(14, 22, 47), 6));
  }

  @Test
  void tTimeWithMicroSecond() {
    long microSec = (14L * 60 * 60 + 22 * 60 + 47) * 1000 * 1000 + 123456;
    long seconds = TimeUnit.MICROSECONDS.toSeconds(microSec);
    int fracSecondsInNano =
        (int) (TimeUnit.MICROSECONDS.toNanos(microSec) - TimeUnit.SECONDS.toNanos(seconds));
    assertEquals(
        TimeString.fromMillisOfDay((int) TimeUnit.SECONDS.toMillis(seconds))
            .withNanos(fracSecondsInNano),
        new TimeString("14:22:47.123456"));

    bitest(
        ExpressionCreator.precisionTime(
            false, (14L * 60 * 60 + 22 * 60 + 47) * 1000 * 1000 + 123456, 6),
        rex.makeTimeLiteral(new TimeString("14:22:47.123456"), 6));
  }

  @Test
  void tTimeWithNanoSecond() {
    assertEquals(
        rex.makeTimeLiteral(new TimeString("14:22:47.123456789"), 9),
        rex.makeTimeLiteral(new TimeString("14:22:47.123456"), 6));
  }

  @Test
  void tPrecisionTimestampBeforeTheEpoch() {
    // 1969-12-31 23:59:59.5 at each precision Calcite can be given. The value is negative and the
    // sub-second part is not, so splitting it has to floor rather than truncate towards zero:
    // TimestampString.withNanos rejects the negative remainder truncation leaves behind.
    assertEquals(new TimestampString("1969-12-31 23:59:59"), timestampStringOf(-1L, 0));
    assertEquals(new TimestampString("1969-12-31 23:59:59.5"), timestampStringOf(-500L, 3));
    assertEquals(new TimestampString("1969-12-31 23:59:59.5"), timestampStringOf(-500_000L, 6));

    // The epoch itself, and a value after it, are unchanged by the same split.
    assertEquals(new TimestampString("1970-01-01 00:00:00"), timestampStringOf(0L, 3));
    assertEquals(new TimestampString("1970-01-01 00:00:01.5"), timestampStringOf(1_500L, 3));
  }

  @Test
  void tPrecisionTimeKeepsItsSubSecondPart() {
    // A time of day is never negative, so this only guards that the shared split did not change
    // what it produced.
    assertEquals(
        new TimeString("14:22:47.5"),
        ((RexLiteral)
                ExpressionCreator.precisionTime(false, (14L * 3600 + 22 * 60 + 47) * 1000 + 500, 3)
                    .accept(expressionRexConverter, Context.newContext()))
            .getValueAs(TimeString.class));
  }

  private TimestampString timestampStringOf(long value, int precision) {
    RexNode converted =
        ExpressionCreator.precisionTimestamp(false, value, precision)
            .accept(expressionRexConverter, Context.newContext());
    return ((RexLiteral) converted).getValueAs(TimestampString.class);
  }

  @Test
  void tDate() {
    bitest(
        ExpressionCreator.date(false, (int) LocalDate.of(2002, 2, 14).toEpochDay()),
        rex.makeDateLiteral(new DateString(2002, 2, 14)));
  }

  @Test
  void tTimestamp() {
    long epochMicro =
        TimeUnit.SECONDS.toMicros(
                LocalDateTime.of(2002, 2, 14, 16, 20, 47).toEpochSecond(ZoneOffset.UTC))
            + 123;
    PrecisionTimestampLiteral ts = ExpressionCreator.precisionTimestamp(false, epochMicro, 6);
    int nano = (int) TimeUnit.MICROSECONDS.toNanos(123);
    TimestampString tsx = new TimestampString(2002, 2, 14, 16, 20, 47).withNanos(nano);
    bitest(ts, rex.makeTimestampLiteral(tsx, 6));
  }

  @Test
  void tTimestampWithMilliMicroSeconds() {
    long epochMicro =
        TimeUnit.SECONDS.toMicros(
                LocalDateTime.of(2002, 2, 14, 16, 20, 47).toEpochSecond(ZoneOffset.UTC))
            + 123456;
    PrecisionTimestampLiteral ts = ExpressionCreator.precisionTimestamp(false, epochMicro, 6);
    int nano = (int) TimeUnit.MICROSECONDS.toNanos(123456);
    TimestampString tsx = new TimestampString(2002, 2, 14, 16, 20, 47).withNanos(nano);
    bitest(ts, rex.makeTimestampLiteral(tsx, 6));
  }

  @Disabled("Not clear what the right literal mapping is.")
  @Test
  void tTimestampTZ() {
    // Calcite has TimestampWithTimeZoneString but it doesn't appear to be available as a literal or
    // data type.
    // (Doesn't exist in SqlTypeName.)
  }

  @Test
  void tIntervalYearMonth() {
    BigDecimal bd = new BigDecimal(3 * 12 + 5); // '3-5' year to month
    RexLiteral intervalYearMonth = rex.makeIntervalLiteral(bd, YEAR_MONTH_INTERVAL);
    IntervalYearLiteral intervalYearMonthExpr = ExpressionCreator.intervalYear(false, 3, 5);
    bitest(intervalYearMonthExpr, intervalYearMonth);
  }

  @Test
  void tIntervalYearMonthWithPrecision() {
    BigDecimal bd = new BigDecimal(123 * 12 + 5); // '123-5' year to month
    RexLiteral intervalYearMonth =
        rex.makeIntervalLiteral(
            bd,
            new SqlIntervalQualifier(
                org.apache.calcite.avatica.util.TimeUnit.YEAR,
                3,
                org.apache.calcite.avatica.util.TimeUnit.MONTH,
                -1,
                SqlParserPos.QUOTED_ZERO));
    IntervalYearLiteral intervalYearMonthExpr = ExpressionCreator.intervalYear(false, 123, 5);

    // rex --> expression
    assertEquals(intervalYearMonthExpr, intervalYearMonth.accept(rexExpressionConverter));

    // expression -> rex
    RexLiteral convertedRex =
        (RexLiteral) intervalYearMonthExpr.accept(expressionRexConverter, Context.newContext());

    // Compare value only. Ignore the precision in SqlIntervalQualifier (which is used to parse
    // input string).
    assertEquals(
        intervalYearMonth.getValueAs(BigDecimal.class).longValue(),
        convertedRex.getValueAs(BigDecimal.class).longValue());
  }

  @ParameterizedTest(name = "P={0} {1}ms")
  @CsvSource({
    // precision, calcite millis, expected days, seconds, subseconds, millis back
    "6,      1500,  0,      1,     500000,      1500",
    "6,     -1500,  0,     -1,    -500000,     -1500",
    "5,      1500,  0,      1,      50000,      1500",
    "4,      1500,  0,      1,       5000,      1500",
    "3,      1500,  0,      1,        500,      1500",
    "3,     -1500,  0,     -1,       -500,     -1500",
    "2,      1500,  0,      1,         50,      1500",
    "1,      1500,  0,      1,          5,      1500",
    // Below millisecond precision the remainder cannot be represented and is dropped, towards
    // zero: the millisecond remainder keeps the dividend's sign, where Duration would report
    // -1500ms as -2 seconds plus a positive 500ms part.
    "0,      1500,  0,      1,          0,      1000",
    "0,     -1500,  0,     -1,          0,     -1000",
    // A value spanning days, so the decomposition is exercised rather than just the seconds field.
    "3, 277629500,  3,  18429,        500, 277629500",
    "3,-277629500, -3, -18429,       -500,-277629500",
    // The spec's maximum day count at the highest precision the type system allows. Scaling the
    // whole value into those units would overflow a long; scaling only the remainder does not.
    "9, 315360000000500, 3650000, 0,  500000000, 315360000000500",
    "9,-315360000000500,-3650000, 0, -500000000,-315360000000500",
  })
  void tIntervalDaySecondKeepsItsPrecision(
      int precision, long calciteMillis, int days, int seconds, long subseconds, long millisBack) {
    RexLiteral calciteInterval =
        rex.makeIntervalLiteral(
            new BigDecimal(calciteMillis), SubstraitTypeSystem.daySecondInterval(precision));
    assertEquals(precision, calciteInterval.getType().getScale());

    IntervalDayLiteral expected =
        ExpressionCreator.intervalDay(false, days, seconds, subseconds, precision);
    assertEquals(expected, calciteInterval.accept(rexExpressionConverter));

    RexNode back = expected.accept(expressionRexConverter, Context.newContext());
    assertEquals(precision, back.getType().getScale());
    assertEquals(new BigDecimal(millisBack), ((RexLiteral) back).getValueAs(BigDecimal.class));
  }

  @Test
  void tIntervalDayLiteralIsNullableWhenTheSubstraitLiteralIs() {
    // The interval literal is the one visit that used to build its Calcite type from a qualifier
    // rather than from the type converter, which lost both the precision bound and this.
    RexNode nullableInterval =
        ExpressionCreator.intervalDay(true, 0, 1, 0, 3)
            .accept(expressionRexConverter, Context.newContext());
    assertTrue(nullableInterval.getType().isNullable());
  }

  @Test
  void tIntervalDaySecondTruncatesBelowMillisecond() {
    // Calcite's interval literals carry milliseconds, so the sub-millisecond part of an
    // interval_day<6> does not survive the conversion. The precision does.
    IntervalDayLiteral withMicros = ExpressionCreator.intervalDay(false, 0, 0, 500_123, 6);
    RexNode convertedRex = withMicros.accept(expressionRexConverter, Context.newContext());
    assertEquals(6, convertedRex.getType().getScale());
    assertEquals(
        ExpressionCreator.intervalDay(false, 0, 0, 500_000, 6),
        convertedRex.accept(rexExpressionConverter));
  }

  @Test
  void tIntervalDay() {
    // Calcite always uses milliseconds
    BigDecimal bd = new BigDecimal(TimeUnit.DAYS.toMillis(5));
    RexLiteral intervalDayLiteral =
        rex.makeIntervalLiteral(
            bd,
            new SqlIntervalQualifier(
                org.apache.calcite.avatica.util.TimeUnit.DAY, -1, null, -1, SqlParserPos.ZERO));
    IntervalDayLiteral intervalDayExpr = ExpressionCreator.intervalDay(false, 5, 0, 0, 6);

    // rex --> expression
    Expression convertedExpr = intervalDayLiteral.accept(rexExpressionConverter);
    assertEquals(intervalDayExpr, convertedExpr);

    // expression -> rex
    RexLiteral convertedRex =
        (RexLiteral) intervalDayExpr.accept(expressionRexConverter, Context.newContext());

    // Compare value only. Ignore the precision in SqlIntervalQualifier in comparison.
    assertEquals(
        intervalDayLiteral.getValueAs(BigDecimal.class), convertedRex.getValueAs(BigDecimal.class));
  }

  @Test
  void tIntervalYear() {
    BigDecimal bd = new BigDecimal(123 * 12); // '123' year(3)
    RexLiteral intervalYear =
        rex.makeIntervalLiteral(
            bd,
            new SqlIntervalQualifier(
                org.apache.calcite.avatica.util.TimeUnit.YEAR,
                3,
                null,
                -1,
                SqlParserPos.QUOTED_ZERO));
    IntervalYearLiteral intervalYearExpr = ExpressionCreator.intervalYear(false, 123, 0);
    // rex --> expression
    assertEquals(intervalYearExpr, intervalYear.accept(rexExpressionConverter));

    // expression -> rex
    RexLiteral convertedRex =
        (RexLiteral) intervalYearExpr.accept(expressionRexConverter, Context.newContext());

    // Compare value only. Ignore the precision in SqlIntervalQualifier in comparison.
    assertEquals(
        intervalYear.getValueAs(BigDecimal.class).longValue(),
        convertedRex.getValueAs(BigDecimal.class).longValue());
  }

  @Test
  void tIntervalMonth() {
    BigDecimal bd = new BigDecimal(123); // '123' month(3)
    RexLiteral intervalMonth =
        rex.makeIntervalLiteral(
            bd,
            new SqlIntervalQualifier(
                org.apache.calcite.avatica.util.TimeUnit.MONTH,
                3,
                null,
                -1,
                SqlParserPos.QUOTED_ZERO));
    IntervalYearLiteral intervalMonthExpr =
        ExpressionCreator.intervalYear(false, 123 / 12, 123 % 12);
    // rex --> expression
    assertEquals(intervalMonthExpr, intervalMonth.accept(rexExpressionConverter));

    // expression -> rex
    RexLiteral convertedRex =
        (RexLiteral) intervalMonthExpr.accept(expressionRexConverter, Context.newContext());

    // Compare value only. Ignore the precision in SqlIntervalQualifier in comparison.
    assertEquals(
        intervalMonth.getValueAs(BigDecimal.class).longValue(),
        convertedRex.getValueAs(BigDecimal.class).longValue());
  }

  @Test
  void tFixedChar() {
    bitest(ExpressionCreator.fixedChar(false, "hello "), c("hello ", SqlTypeName.CHAR));
  }

  @Test
  void tVarChar() {
    bitest(ExpressionCreator.varChar(false, "hello ", 10), c("hello ", SqlTypeName.VARCHAR, 10));
  }

  @Test
  void tDecimalLiteral() {
    List<BigDecimal> decimalList =
        List.of(
            new BigDecimal("-123.457890"),
            new BigDecimal("123.457890"),
            new BigDecimal("123.450000"),
            new BigDecimal("-123.450000"));
    for (BigDecimal bd : decimalList) {
      bitest(ExpressionCreator.decimal(false, bd, 32, 6), c(bd, SqlTypeName.DECIMAL, 32, 6));
    }
  }

  @Test
  void tDecimalLiteral2() {
    List<BigDecimal> decimalList =
        List.of(
            new BigDecimal("-99.123456789123456789123456789123456789"), // scale = 36, precision =38
            new BigDecimal("99.123456789123456789123456789123456789") // scale = 36, precision = 38
            );
    for (BigDecimal bd : decimalList) {
      bitest(ExpressionCreator.decimal(false, bd, 38, 36), c(bd, SqlTypeName.DECIMAL, 38, 36));
    }
  }

  @Test
  void tDecimalUtil() {
    long[] values =
        new long[] {Long.MIN_VALUE, Integer.MIN_VALUE, 0, Integer.MAX_VALUE, Long.MAX_VALUE};
    for (long value : values) {
      BigDecimal bd = BigDecimal.valueOf(value);
      byte[] encoded = DecimalUtil.encodeDecimalIntoBytes(bd, 0, 16);
      BigDecimal bd2 = DecimalUtil.getBigDecimalFromBytes(encoded, 0, 16);
      System.out.println(bd2);
      assertEquals(bd, bd2);
    }
  }

  @Test
  void tMap() {
    ImmutableMap<Literal, Literal> ss =
        ImmutableMap.of(
            ExpressionCreator.string(false, "foo"),
            ExpressionCreator.i32(false, 4),
            ExpressionCreator.string(false, "bar"),
            ExpressionCreator.i32(false, -1));
    RexNode calcite =
        rex.makeLiteral(
            ImmutableMap.of("foo", 4, "bar", -1),
            type.createMapType(t(SqlTypeName.VARCHAR), t(SqlTypeName.INTEGER)),
            true,
            false);
    bitest(ExpressionCreator.map(false, ss), calcite);
  }

  @Test
  void tList() {
    bitest(
        ExpressionCreator.list(
            false, ExpressionCreator.i32(false, 4), ExpressionCreator.i32(false, -1)),
        rex.makeLiteral(
            Arrays.asList(4, -1), type.createArrayType(t(SqlTypeName.INTEGER), -1), false, false));
  }

  @Test
  void tStruct() {
    test(
        ExpressionCreator.struct(
            false, ExpressionCreator.i32(false, 4), ExpressionCreator.i32(false, -1)),
        rex.makeLiteral(
            Arrays.asList(4, -1),
            type.createStructType(
                Arrays.asList(t(SqlTypeName.INTEGER), t(SqlTypeName.INTEGER)),
                Arrays.asList("c1", "c2")),
            false,
            false));
  }

  @Test
  void tStructUsesResultFieldTypes() {
    RelDataType literalType = type.createStructType(List.of(t(SqlTypeName.TINYINT)), List.of("c1"));
    RexLiteral literal = (RexLiteral) rex.makeLiteral(List.of(4), literalType, false, false);
    RelDataType resultType = type.createStructType(List.of(tN(SqlTypeName.INTEGER)), List.of("c1"));

    assertEquals(
        ExpressionCreator.struct(false, ExpressionCreator.i32(true, 4)),
        new LiteralConverter(TypeConverter.DEFAULT).convert(literal, resultType));
  }

  @Test
  void tStructRoundtripNullableFields() {
    // Test regular struct with nullable fields roundtrips correctly
    Expression.StructLiteral struct =
        ExpressionCreator.struct(
            false, ExpressionCreator.i32(true, 4), ExpressionCreator.i32(true, -1));

    RexNode rex = struct.accept(expressionRexConverter, Context.newContext());
    Expression roundtrip = rex.accept(rexExpressionConverter);

    assertEquals(struct, roundtrip);
  }

  @Test
  void tStructRoundtripMixedFieldNullability() {
    // Test regular struct with mixed field nullability roundtrips correctly
    Expression.StructLiteral struct =
        ExpressionCreator.struct(
            false, ExpressionCreator.i32(true, 4), ExpressionCreator.i32(false, -1));

    RexNode rex = struct.accept(expressionRexConverter, Context.newContext());
    Expression roundtrip = rex.accept(rexExpressionConverter);

    assertEquals(struct, roundtrip);
  }

  @Test
  void tStructRoundtripWithNullFieldValues() {
    // Test struct with actual NULL field values roundtrips correctly
    Expression.NullLiteral nullField =
        ExpressionCreator.typedNull(io.substrait.type.Type.I32.builder().nullable(true).build());

    Expression.StructLiteral struct =
        ExpressionCreator.struct(false, nullField, ExpressionCreator.i32(false, 100));

    RexNode rex = struct.accept(expressionRexConverter, Context.newContext());
    Expression roundtrip = rex.accept(rexExpressionConverter);

    assertEquals(struct, roundtrip);
  }

  @Test
  void tStructRoundtripNested() {
    // Test nested regular structs roundtrip correctly
    Expression.StructLiteral innerStruct =
        ExpressionCreator.struct(
            false, ExpressionCreator.i32(false, 1), ExpressionCreator.i32(false, 2));

    Expression.StructLiteral outerStruct =
        ExpressionCreator.struct(false, innerStruct, ExpressionCreator.i32(false, 3));

    RexNode rex = outerStruct.accept(expressionRexConverter, Context.newContext());
    Expression roundtrip = rex.accept(rexExpressionConverter);

    assertEquals(outerStruct, roundtrip);
  }

  @Test
  void tStructRoundtripEmpty() {
    // Test empty struct roundtrips correctly
    Expression.StructLiteral struct = ExpressionCreator.struct(false, Collections.emptyList());

    RexNode rex = struct.accept(expressionRexConverter, Context.newContext());
    Expression roundtrip = rex.accept(rexExpressionConverter);

    assertEquals(struct, roundtrip);
  }

  @Test
  void tFixedBinary() {
    byte[] val = "my test".getBytes(StandardCharsets.UTF_8);
    bitest(
        ExpressionCreator.fixedBinary(false, val),
        c(new org.apache.calcite.avatica.util.ByteString(val), SqlTypeName.BINARY));
  }

  public void test(Expression expression, RexNode rex) {
    assertEquals(expression, rex.accept(new RexExpressionConverter()));
  }

  // bi-directional test : 1) rex -> substrait,  substrait -> rex2.  Compare rex == rex2
  public void bitest(Expression expression, RexNode rex) {
    assertEquals(expression, rex.accept(rexExpressionConverter));
    RexNode convertedRex = expression.accept(expressionRexConverter, Context.newContext());
    assertEquals(rex, convertedRex);
  }
}
