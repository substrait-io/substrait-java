package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
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
                    creator.TIME,
                    creator.DATE,
                    creator.TIMESTAMP,
                    creator.TIMESTAMP_TZ,
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
                    creator.list(creator.TIME),
                    creator.struct(creator.TIME, creator.TIMESTAMP, creator.TIMESTAMP_TZ)));
  }

  @ParameterizedTest
  @MethodSource("types")
  @DisplayName("pojo -> proto -> pojo round trip")
  void roundtripFromType(Type type) {
    io.substrait.proto.Type converted = type.accept(typeProtoConverter);
    Type actual = protoTypeConverter.from(converted);
    assertEquals(type, actual);
  }
}
