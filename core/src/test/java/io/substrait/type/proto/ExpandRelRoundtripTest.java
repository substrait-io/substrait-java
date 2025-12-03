package io.substrait.type.proto;

import io.substrait.TestBase;
import io.substrait.hint.Hint;
import io.substrait.relation.Expand;
import io.substrait.relation.Rel;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class ExpandRelRoundtripTest extends TestBase {
  final Rel input =
      b.namedScan(
          Stream.of("a_table").collect(Collectors.toList()),
          Stream.of("column1", "column2").collect(Collectors.toList()),
          Stream.of(R.I64, R.I64).collect(Collectors.toList()));

  private Expand.ExpandField getConsistentField(final int index) {
    return Expand.ConsistentField.builder().expression(b.fieldReference(input, index)).build();
  }

  private Expand.ExpandField getSwitchingField(final List<Integer> indexes) {
    return Expand.SwitchingField.builder()
        .addAllDuplicates(
            indexes.stream()
                .map(index -> b.fieldReference(input, index))
                .collect(Collectors.toList()))
        .build();
  }

  @Test
  void expandConsistent() {
    final Rel rel =
        Expand.builder()
            .from(b.expand(__ -> Collections.emptyList(), input))
            .hint(
                Hint.builder()
                    .alias("alias1")
                    .addAllOutputNames(Arrays.asList("name1", "name2"))
                    .build())
            .fields(
                Stream.of(getConsistentField(0), getConsistentField(1))
                    .collect(Collectors.toList()))
            .build();
    verifyRoundTrip(rel);
  }

  @Test
  void expandSwitching() {
    final Rel rel =
        Expand.builder()
            .from(b.expand(__ -> Collections.emptyList(), input))
            .hint(Hint.builder().addAllOutputNames(Arrays.asList("name1", "name2")).build())
            .fields(
                Stream.of(
                        getSwitchingField(Arrays.asList(0, 1)),
                        getSwitchingField(Arrays.asList(1, 0)))
                    .collect(Collectors.toList()))
            .build();
    verifyRoundTrip(rel);
  }
}
