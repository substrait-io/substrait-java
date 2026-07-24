package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestTypeRoundtrip {

  private static ExtensionCollector lookup = new ExtensionCollector();
  private static TypeProtoConverter typeProtoConverter = new TypeProtoConverter(lookup);
  private static ProtoTypeConverter protoTypeConverter =
      new ProtoTypeConverter(lookup, SimpleExtension.ExtensionCollection.builder().build());

  static Stream<Type> types() {
    return Stream.of(true, false)
        .map(nullable -> nullable ? TypeCreator.NULLABLE : TypeCreator.REQUIRED)
        .flatMap(
            creator ->
                Stream.of(
                    creator.BOOLEAN,
                    creator.I8,
                    creator.I16,
                    creator.I32,
                    creator.I64,
                    creator.FP32,
                    creator.FP64,
                    creator.STRING,
                    creator.BINARY,
                    creator.DATE,
                    creator.INTERVAL_YEAR,
                    creator.UUID,
                    creator.fixedChar(25),
                    creator.varChar(35),
                    creator.fixedBinary(45),
                    creator.decimal(34, 3),
                    creator.intervalDay(6),
                    creator.intervalCompound(3),
                    creator.precisionTime(3),
                    creator.precisionTimestamp(1),
                    creator.precisionTimestampTZ(2),
                    creator.map(creator.I8, creator.I16),
                    creator.list(creator.DATE),
                    creator.struct(
                        creator.DATE,
                        creator.precisionTimestamp(6),
                        creator.precisionTimestampTZ(6))));
  }

  @ParameterizedTest
  @MethodSource("types")
  @DisplayName("pojo -> proto -> pojo round trip")
  void roundtripFromType(Type type) {
    io.substrait.proto.Type converted = type.accept(typeProtoConverter);
    Type actual = protoTypeConverter.from(converted);
    assertEquals(type, actual);
  }

  @Test
  @DisplayName("interval day type with an unset precision is rejected")
  void intervalDayWithoutPrecisionIsRejected() {
    io.substrait.proto.Type protoType =
        io.substrait.proto.Type.newBuilder()
            .setIntervalDay(
                io.substrait.proto.Type.IntervalDay.newBuilder()
                    .setNullability(io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED))
            .build();
    assertThrows(IllegalArgumentException.class, () -> protoTypeConverter.from(protoType));
  }
}
