package io.substrait.isthmus;

import static io.substrait.expression.ExpressionCreator.*;
import static io.substrait.isthmus.SqlToSubstrait.EXTENSION_COLLECTION;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import io.substrait.expression.Expression;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.type.Type;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class CalciteLiteralTest extends CalciteObjs {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(CalciteLiteralTest.class);

  private final ScalarFunctionConverter scalarFunctionConverter =
      new ScalarFunctionConverter(EXTENSION_COLLECTION.scalarFunctions(), type);

  private final ExpressionRexConverter expressionRexConverter =
      new ExpressionRexConverter(type, scalarFunctionConverter);

  @Test
  void nullLiteral() {
    bitest(typedNull(Type.NULLABLE.varChar(10)), rex.makeNullLiteral(tN(SqlTypeName.VARCHAR, 10)));
  }

  @Test
  void tI8() {
    bitest(i8(false, 4), c(4, SqlTypeName.TINYINT));
  }

  @Test
  void tI16() {
    bitest(i16(false, 4), c(4, SqlTypeName.SMALLINT));
  }

  @Test
  void tI32() {
    bitest(i32(false, 4), c(4, SqlTypeName.INTEGER));
  }

  @Test
  void tI64() {
    bitest(i64(false, 1234L), c(1234L, SqlTypeName.BIGINT));
  }

  @Test
  void tFP32() {
    bitest(fp32(false, 4.44F), c(4.44F, SqlTypeName.FLOAT));
  }

  @Test
  void tFP64() {
    bitest(fp64(false, 4.45F), c(4.45F, SqlTypeName.DOUBLE));
  }

  @Test
  void tStr() {
    // TODO: varchar vs fixed length char
    test(string(false, "my test"), c("my test", SqlTypeName.VARCHAR));
  }

  @Test
  void tBinary() {
    // TODO: varbinary vs fixed length binary
    var val = "my test".getBytes(StandardCharsets.UTF_8);
    test(
        binary(false, val),
        c(new org.apache.calcite.avatica.util.ByteString(val), SqlTypeName.VARBINARY));
  }

  @Test
  void tTime() {
    bitest(
        time(false, (14L * 60 * 60 + 22 * 60 + 47) * 1000 * 1000),
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
        time(false, (14L * 60 * 60 + 22 * 60 + 47) * 1000 * 1000 + 123456),
        rex.makeTimeLiteral(new TimeString("14:22:47.123456"), 6));
  }

  @Test
  void tTimeWithNanoSecond() {
    assertEquals(
        rex.makeTimeLiteral(new TimeString("14:22:47.123456789"), 9),
        rex.makeTimeLiteral(new TimeString("14:22:47.123456"), 6));
  }

  @Test
  void tDate() {
    bitest(
        date(false, (int) LocalDate.of(2002, 2, 14).toEpochDay()),
        rex.makeDateLiteral(new DateString(2002, 2, 14)));
  }

  @Test
  void tTimestamp() {
    var ts = timestamp(false, 2002, 2, 14, 16, 20, 47, 123);
    var nano = (int) TimeUnit.MICROSECONDS.toNanos(123);
    var tsx = new TimestampString(2002, 2, 14, 16, 20, 47).withNanos(nano);
    bitest(ts, rex.makeTimestampLiteral(tsx, 6));
  }

  @Test
  void tTimestampWithMilliMacroSeconds() {
    var ts = timestamp(false, 2002, 2, 14, 16, 20, 47, 123456);
    var nano = (int) TimeUnit.MICROSECONDS.toNanos(123456);
    var tsx = new TimestampString(2002, 2, 14, 16, 20, 47).withNanos(nano);
    bitest(ts, rex.makeTimestampLiteral(tsx, 6));
  }

  @Disabled("Not clear what the right literal mapping is.")
  @Test
  void tTimestampTZ() {
    // Calcite has TimestampWithTimeZoneString but it doesn't appear to be available as a literal or
    // data type.
    // (Doesn't exist in SqlTypeName.)
  }

  @Disabled("NYI")
  @Test
  void tIntervalYear() {}

  @Disabled("NYI")
  @Test
  void tIntervalDay() {}

  @Test
  void tFixedChar() {
    test(fixedChar(false, "hello "), c("hello ", SqlTypeName.CHAR));
  }

  @Test
  void tVarChar() {
    test(varChar(false, "hello ", 10), c("hello ", SqlTypeName.VARCHAR, 10));
  }

  @Test
  void tDecimalLiteral() {
    BigDecimal bd = BigDecimal.valueOf(-123.45789);
    test(decimal(false, bd, 32, 10), c(bd, SqlTypeName.DECIMAL, 32, 10));
  }

  @Test
  void tMap() {
    var ss =
        ImmutableMap.<Expression.Literal, Expression.Literal>of(
            string(false, "foo"), i32(false, 4), string(false, "bar"), i32(false, -1));
    var calcite =
        rex.makeLiteral(
            ImmutableMap.of("foo", 4, "bar", -1),
            type.createMapType(t(SqlTypeName.VARCHAR), t(SqlTypeName.INTEGER)),
            true,
            false);
    test(map(false, ss), calcite);
  }

  @Test
  void tList() {
    test(
        list(false, i32(false, 4), i32(false, -1)),
        rex.makeLiteral(
            Arrays.asList(4, -1), type.createArrayType(t(SqlTypeName.INTEGER), -1), false, false));
  }

  @Test
  void tStruct() {
    test(
        struct(false, i32(false, 4), i32(false, -1)),
        rex.makeLiteral(
            Arrays.asList(4, -1),
            type.createStructType(
                Arrays.asList(t(SqlTypeName.INTEGER), t(SqlTypeName.INTEGER)),
                Arrays.asList("c1", "c2")),
            false,
            false));
  }

  @Test
  void tFixedBinary() {
    var val = "my test".getBytes(StandardCharsets.UTF_8);
    test(
        fixedBinary(false, val),
        c(new org.apache.calcite.avatica.util.ByteString(val), SqlTypeName.BINARY));
  }

  public void test(Expression expression, RexNode rex) {
    assertEquals(expression, rex.accept(new RexExpressionConverter()));
  }

  // bi-directional test : 1) rex -> substrait,  substrait -> rex2.  Compare rex == rex2
  public void bitest(Expression expression, RexNode rex) {
    assertEquals(expression, rex.accept(new RexExpressionConverter()));

    RexNode convertedRex = expression.accept(expressionRexConverter);
    assertEquals(rex, convertedRex);
  }
}
