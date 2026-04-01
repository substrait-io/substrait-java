package io.substrait.isthmus;

import com.google.common.collect.Streams;
import io.substrait.relation.Aggregate;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class AggregationFunctionsTest extends PlanTestBase {

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
  private NamedScan numericTypesTable = sb.namedScan(List.of("example"), columnNames, tableTypes);

  // Create the given function call on the given field of the input
  private Aggregate.Measure functionPicker(Rel input, int field, String fname) {
    switch (fname) {
      case "min":
        return sb.min(input, field);
      case "max":
        return sb.max(input, field);
      case "sum":
        return sb.sum(input, field);
      case "sum0":
        return sb.sum0(input, field);
      case "avg":
        return sb.avg(input, field);
      case "stddev_pop":
        return sb.stddevPopulation(input, field);
      case "stddev_samp":
        return sb.stddevSample(input, field);
      case "var_pop":
        return sb.variancePopulation(input, field);
      case "var_samp":
        return sb.varianceSample(input, field);
      default:
        throw new UnsupportedOperationException(
            String.format("no function is associated with %s", fname));
    }
  }

  // Create one function call per numeric type column
  private List<Aggregate.Measure> functions(Rel input, String fname) {
    // first column is for grouping, skip it
    // Statistical functions (stddev_*, var_*) only support floating-point types in Substrait.
    // This filtering ensures we only test with fp32 and fp64 types for these functions,
    // avoiding type mismatch errors during round-trip conversion.
    boolean isStatisticalFunction = fname.startsWith("stddev_") || fname.startsWith("var_");
    return IntStream.range(1, tableTypes.size())
        .boxed()
        .filter(
            index -> {
              if (!isStatisticalFunction) {
                return true; // All numeric types for non-statistical functions
              }
              // Only floating-point types for statistical functions
              Type type = tableTypes.get(index);
              return type.equals(R.FP32)
                  || type.equals(R.FP64)
                  || type.equals(N.FP32)
                  || type.equals(N.FP64);
            })
        .map(index -> functionPicker(input, index, fname))
        .collect(Collectors.toList());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "max",
        "min",
        "sum",
        "sum0",
        "avg",
        "stddev_pop",
        "stddev_samp",
        "var_pop",
        "var_samp"
      })
  void emptyGrouping(String aggFunction) {
    Aggregate rel =
        sb.aggregate(
            input -> sb.grouping(input), input -> functions(input, aggFunction), numericTypesTable);
    assertFullRoundTrip(rel);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "max",
        "min",
        "sum",
        "sum0",
        "avg",
        "stddev_pop",
        "stddev_samp",
        "var_pop",
        "var_samp"
      })
  void withGrouping(String aggFunction) {
    Aggregate rel =
        sb.aggregate(
            input -> sb.grouping(input, 0),
            input -> functions(input, aggFunction),
            numericTypesTable);
    assertFullRoundTrip(rel);
  }
}
