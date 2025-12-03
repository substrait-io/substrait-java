package io.substrait.relation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SetTest {

  private static final Type N = TypeCreator.NULLABLE.BOOLEAN;
  private static final Type R = TypeCreator.REQUIRED.BOOLEAN;

  @Test
  void deriveRecordTypeNullability() {
    final List<String> names =
        Arrays.asList("col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8");

    // From https://substrait.io/relations/logical_relations/#output-type-derivation-examples
    final EmptyScan input1 =
        EmptyScan.builder()
            .initialSchema(NamedStruct.of(names, getStruct(R, R, R, R, N, N, N, N)))
            .build();
    final EmptyScan input2 =
        EmptyScan.builder()
            .initialSchema(NamedStruct.of(names, getStruct(R, R, N, N, R, R, N, N)))
            .build();
    final EmptyScan input3 =
        EmptyScan.builder()
            .initialSchema(NamedStruct.of(names, getStruct(R, N, R, N, R, N, R, N)))
            .build();

    final Map<Set.SetOp, Type.Struct> expecteds = new HashMap<>();
    expecteds.put(Set.SetOp.MINUS_PRIMARY, getStruct(R, R, R, R, N, N, N, N));
    expecteds.put(Set.SetOp.MINUS_PRIMARY_ALL, getStruct(R, R, R, R, N, N, N, N));
    expecteds.put(Set.SetOp.MINUS_MULTISET, getStruct(R, R, R, R, N, N, N, N));
    expecteds.put(Set.SetOp.INTERSECTION_PRIMARY, getStruct(R, R, R, R, R, N, N, N));
    expecteds.put(Set.SetOp.INTERSECTION_MULTISET, getStruct(R, R, R, R, R, R, R, N));
    expecteds.put(Set.SetOp.INTERSECTION_MULTISET_ALL, getStruct(R, R, R, R, R, R, R, N));
    expecteds.put(Set.SetOp.UNION_DISTINCT, getStruct(R, N, N, N, N, N, N, N));
    expecteds.put(Set.SetOp.UNION_ALL, getStruct(R, N, N, N, N, N, N, N));

    Arrays.stream(Set.SetOp.values())
        .forEach(
            setOp -> {
              if (setOp == Set.SetOp.UNKNOWN) {
                return;
              }
              final Type.Struct expected = expecteds.get(setOp);
              assertNotNull(expected, "Missing expected record type for " + setOp);
              assertEquals(
                  expected,
                  Set.builder()
                      .addInputs(input1, input2, input3)
                      .setOp(setOp)
                      .build()
                      .getRecordType(),
                  "Mismatch for " + setOp);
            });
  }

  private Type.Struct getStruct(final Type... types) {
    return Type.Struct.builder().addFields(types).nullable(false).build();
  }
}
