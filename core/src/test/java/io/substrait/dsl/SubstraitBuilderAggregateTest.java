package io.substrait.dsl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.expression.FieldReference;
import io.substrait.relation.Aggregate;
import io.substrait.relation.NamedScan;
import io.substrait.type.Type;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SubstraitBuilderAggregateTest extends TestBase {

  static Stream<Arguments> numericTypes() {
    return Stream.of(R, N)
        .flatMap(
            creator ->
                Stream.of(
                    Arguments.of(creator.I8, R.I64),
                    Arguments.of(creator.I16, R.I64),
                    Arguments.of(creator.I32, R.I64),
                    Arguments.of(creator.I64, R.I64),
                    Arguments.of(creator.FP32, R.FP64),
                    Arguments.of(creator.FP64, R.FP64)));
  }

  static Stream<Type> numericInputTypes() {
    return numericTypes().map(arguments -> (Type) arguments.get()[0]);
  }

  @ParameterizedTest
  @MethodSource("numericTypes")
  void sumWidensToNullableResult(Type inputType, Type widenedType) {
    NamedScan scan = sb.namedScan(List.of("t"), List.of("v"), List.of(inputType));
    FieldReference input = sb.fieldReference(scan, 0);
    Aggregate.Measure byExpression = sb.sum(input);
    Aggregate.Measure byField = sb.sum(scan, 0);

    assertEquals("sum", byExpression.getFunction().declaration().name());
    assertEquals(widenedType.withNullable(true), byExpression.getFunction().outputType());
    assertEquals(byExpression, byField);
    verifyRoundTrip(
        Aggregate.builder().input(scan).addGroupings(sb.grouping()).addMeasures(byField).build());
  }

  @ParameterizedTest
  @MethodSource("numericTypes")
  void sum0WidensToRequiredResult(Type inputType, Type widenedType) {
    NamedScan scan = sb.namedScan(List.of("t"), List.of("v"), List.of(inputType));
    FieldReference input = sb.fieldReference(scan, 0);
    Aggregate.Measure byExpression = sb.sum0(input);
    Aggregate.Measure byField = sb.sum0(scan, 0);

    assertEquals("sum0", byExpression.getFunction().declaration().name());
    assertEquals(widenedType, byExpression.getFunction().outputType());
    assertEquals(byExpression, byField);
    verifyRoundTrip(
        Aggregate.builder().input(scan).addGroupings(sb.grouping()).addMeasures(byField).build());
  }

  @ParameterizedTest
  @MethodSource("numericInputTypes")
  void minMaxAndAvgKeepNullableInputType(Type inputType) {
    NamedScan scan = sb.namedScan(List.of("t"), List.of("v"), List.of(inputType));
    FieldReference input = sb.fieldReference(scan, 0);
    List<Aggregate.Measure> measures = List.of(sb.min(input), sb.max(input), sb.avg(input));

    assertEquals(List.of(sb.min(scan, 0), sb.max(scan, 0), sb.avg(scan, 0)), measures);
    for (Aggregate.Measure measure : measures) {
      assertEquals(inputType.withNullable(true), measure.getFunction().outputType());
    }
    verifyRoundTrip(
        Aggregate.builder().input(scan).addGroupings(sb.grouping()).measures(measures).build());
  }
}
