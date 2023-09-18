package io.substrait.isthmus;

import com.google.common.collect.Streams;
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class AggregationFunctionsTest extends PlanTestBase {

  SubstraitBuilder b = new SubstraitBuilder(extensions);

  static final TypeCreator R = TypeCreator.of(false);
  static final TypeCreator N = TypeCreator.of(true);

  // Create a table with that has a column of every numeric type, both NOT NULL and NULL
  private List<Type> numericTypesR = List.of(R.I8, R.I16, R.I32, R.I64, R.FP32, R.FP64);
  private List<Type> numericTypesN = List.of(N.I8, N.I16, N.I32, N.I64, N.FP32, N.FP64);
  private List<Type> numericTypes =
      Stream.concat(numericTypesR.stream(), numericTypesN.stream()).collect(Collectors.toList());

  private List<Type> tableTypes =
      Stream.concat(
              // Column to Group By
              Stream.of(N.I8),
              // Columns with Numeric Types
              numericTypes.stream())
          .collect(Collectors.toList());
  private List<String> columnNames =
      Streams.mapWithIndex(tableTypes.stream(), (t, index) -> String.valueOf(index))
          .collect(Collectors.toList());
  private NamedScan numericTypesTable = b.namedScan(List.of("example"), columnNames, tableTypes);

  // Create the given function call on the given field of the input
  private AggregateFunctionInvocation functionPicker(Rel input, int field, String fname) {
    return switch (fname) {
      case "min" -> b.min(input, field);
      case "max" -> b.max(input, field);
      case "sum" -> b.sum(input, field);
      case "sum0" -> b.sum0(input, field);
      case "avg" -> b.avg(input, field);
      default -> throw new RuntimeException(
          String.format("no function is associated with %s", fname));
    };
  }

  // Create one function call per numeric type column
  private List<AggregateFunctionInvocation> functions(Rel input, String fname) {
    // first column is for grouping, skip it
    return IntStream.range(1, tableTypes.size())
        .boxed()
        .map(index -> functionPicker(input, index, fname))
        .collect(Collectors.toList());
  }

  @ParameterizedTest
  @ValueSource(strings = {"max", "min", "sum", "sum0", "avg"})
  void emptyGrouping(String aggFunction) {
    var rel =
        b.aggregate(
            input -> b.grouping(input), input -> functions(input, aggFunction), numericTypesTable);
    assertFullRoundTrip(rel);
  }

  @ParameterizedTest
  @ValueSource(strings = {"max", "min", "sum", "sum0", "avg"})
  void withGrouping(String aggFunction) {
    var rel =
        b.aggregate(
            input -> b.grouping(input, 0),
            input -> functions(input, aggFunction),
            numericTypesTable);
    assertFullRoundTrip(rel);
  }
}
