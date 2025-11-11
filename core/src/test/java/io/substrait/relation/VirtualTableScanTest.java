package io.substrait.relation;

import static io.substrait.expression.ExpressionCreator.i32;
import static io.substrait.expression.ExpressionCreator.i64;
import static io.substrait.expression.ExpressionCreator.list;
import static io.substrait.expression.ExpressionCreator.map;
import static io.substrait.expression.ExpressionCreator.string;
import static io.substrait.expression.ExpressionCreator.struct;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.type.NamedStruct;
import io.substrait.type.TypeCreator;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class VirtualTableScanTest extends TestBase {

  @Test
  void check() {
    VirtualTableScan virtualTableScan =
        ImmutableVirtualTableScan.builder()
            .initialSchema(
                NamedStruct.of(
                    Arrays.stream(
                            new String[] {
                              "string",
                              "struct",
                              "struct_field1",
                              "struct_field2",
                              "list",
                              "list_struct_field1",
                              "map",
                              "map_key_struct_field1",
                              "map_value_struct_field1"
                            })
                        .collect(Collectors.toList()),
                    R.struct(
                        R.STRING,
                        R.struct(R.STRING, R.STRING),
                        R.list(R.struct(R.STRING)),
                        R.map(R.struct(R.STRING), R.struct(R.STRING)))))
            .addRows(
                struct(
                    false,
                    string(false, "string_val"),
                    struct(
                        false,
                        string(false, "struct_field1_val"),
                        string(false, "struct_field2_val")),
                    list(false, struct(false, string(false, "list_struct_field1_val"))),
                    map(
                        false,
                        mapOf(
                            struct(false, string(false, "map_key_struct_field1_val")),
                            struct(false, string(false, "map_value_struct_field1_val"))))))
            .build();
    assertDoesNotThrow(virtualTableScan::check);
  }

  @Test
  void checkValidRowsWithSimpleTypes() {
    // Test with simple types and multiple rows
    VirtualTableScan virtualTableScan =
        ImmutableVirtualTableScan.builder()
            .initialSchema(
                NamedStruct.of(
                    Arrays.asList("id", "name", "age"), R.struct(R.I64, R.STRING, R.I32)))
            .addRows(
                struct(false, i64(false, 1L), string(false, "Alice"), i32(false, 30)),
                struct(false, i64(false, 2L), string(false, "Bob"), i32(false, 25)))
            .build();
    assertDoesNotThrow(virtualTableScan::check);
  }

  @Test
  void checkInvalidRowTypeMismatch() {
    // Row has I32 where schema expects STRING
    assertThrows(
        AssertionError.class,
        () ->
            ImmutableVirtualTableScan.builder()
                .initialSchema(
                    NamedStruct.of(
                        Arrays.asList("id", "name", "age"), R.struct(R.I64, R.STRING, R.I32)))
                .addRows(
                    struct(
                        false,
                        i64(false, 1L),
                        i32(false, 123), // Wrong type - should be STRING
                        i32(false, 30)))
                .build());
  }

  @Test
  void checkInvalidRowWrongFieldCount() {
    // Row has wrong number of fields
    assertThrows(
        AssertionError.class,
        () ->
            ImmutableVirtualTableScan.builder()
                .initialSchema(
                    NamedStruct.of(
                        Arrays.asList("id", "name", "age"), R.struct(R.I64, R.STRING, R.I32)))
                .addRows(struct(false, i64(false, 1L), string(false, "Alice"))) // Missing age field
                .build());
  }

  @Test
  void checkInvalidNestedStructTypeMismatch() {
    // Nested struct has wrong field type
    assertThrows(
        AssertionError.class,
        () ->
            ImmutableVirtualTableScan.builder()
                .initialSchema(
                    NamedStruct.of(
                        Arrays.asList("id", "details", "field1", "field2"),
                        R.struct(R.I64, R.struct(R.STRING, R.I32))))
                .addRows(
                    struct(
                        false,
                        i64(false, 1L),
                        struct(
                            false,
                            string(false, "value"),
                            string(false, "wrong")))) // Should be I32, not STRING
                .build());
  }

  @Test
  void checkInvalidListElementTypeMismatch() {
    // List has wrong element type
    assertThrows(
        AssertionError.class,
        () ->
            ImmutableVirtualTableScan.builder()
                .initialSchema(
                    NamedStruct.of(Arrays.asList("id", "tags"), R.struct(R.I64, R.list(R.STRING))))
                .addRows(
                    struct(
                        false,
                        i64(false, 1L),
                        list(false, i32(false, 123)))) // Should be STRING, not I32
                .build());
  }

  @Test
  void checkInvalidNullabilityMismatch() {
    // Nullability must match exactly
    assertThrows(
        AssertionError.class,
        () ->
            ImmutableVirtualTableScan.builder()
                .initialSchema(
                    NamedStruct.of(
                        Arrays.asList("id", "name"),
                        TypeCreator.NULLABLE.struct(
                            TypeCreator.NULLABLE.I64, TypeCreator.NULLABLE.STRING)))
                .addRows(
                    struct(
                        false,
                        i64(false, 1L), // non-nullable doesn't match nullable schema
                        string(false, "Alice")))
                .build());
  }

  private Map<Expression.Literal, Expression.Literal> mapOf(
      Expression.Literal key, Expression.Literal value) {
    // Map.of() comes only in Java 9 and the "core" module is on Java 8
    HashMap<Expression.Literal, Expression.Literal> map = new HashMap<>();
    map.put(key, value);
    return map;
  }
}
