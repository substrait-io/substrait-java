package io.substrait.function;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ToTypeStringTest {

  static Stream<Arguments> types() {
    return Stream.of(TypeCreator.REQUIRED, TypeCreator.NULLABLE)
        .flatMap(
            c ->
                Stream.of(
                    Arguments.of(c.BOOLEAN, "bool"),
                    Arguments.of(c.I8, "i8"),
                    Arguments.of(c.I16, "i16"),
                    Arguments.of(c.I32, "i32"),
                    Arguments.of(c.I64, "i64"),
                    Arguments.of(c.FP32, "fp32"),
                    Arguments.of(c.FP64, "fp64"),
                    Arguments.of(c.STRING, "str"),
                    Arguments.of(c.BINARY, "vbin"),
                    Arguments.of(c.DATE, "date"),
                    Arguments.of(c.TIME, "time"),
                    Arguments.of(c.TIMESTAMP, "ts"),
                    Arguments.of(c.TIMESTAMP_TZ, "tstz"),
                    Arguments.of(c.INTERVAL_YEAR, "iyear"),
                    Arguments.of(c.UUID, "uuid"),
                    Arguments.of(c.fixedChar(1), "fchar"),
                    Arguments.of(c.varChar(1), "vchar"),
                    Arguments.of(c.fixedBinary(1), "fbin"),
                    Arguments.of(c.decimal(10, 2), "dec"),
                    Arguments.of(c.intervalDay(6), "iday"),
                    Arguments.of(c.intervalCompound(3), "icompound"),
                    Arguments.of(c.precisionTime(3), "pt"),
                    Arguments.of(c.precisionTimestamp(3), "pts"),
                    Arguments.of(c.precisionTimestampTZ(3), "ptstz"),
                    Arguments.of(c.struct(c.I32), "struct"),
                    Arguments.of(c.list(c.I32), "list"),
                    Arguments.of(c.map(c.I32, c.I64), "map")));
  }

  @ParameterizedTest
  @MethodSource("types")
  void typeToShortName(Type type, String expected) {
    assertEquals(expected, ToTypeString.apply(type));
  }

  @Test
  void userDefined() {
    assertEquals("u!point", ToTypeString.apply(TypeCreator.REQUIRED.userDefined("urn", "point")));
  }

  @Test
  void anyCollapsesNumericSuffix() {
    ParameterizedType.StringLiteral any1 =
        ParameterizedType.StringLiteral.builder().nullable(false).value("any1").build();
    assertEquals("any", any1.accept(ToTypeString.INSTANCE));
  }

  @Test
  void losslessPreservesNumericSuffix() {
    ParameterizedType.StringLiteral any1 =
        ParameterizedType.StringLiteral.builder().nullable(false).value("any1").build();
    assertEquals("any1", any1.accept(ToTypeString.ToTypeLiteralStringLossless.INSTANCE));
  }
}
