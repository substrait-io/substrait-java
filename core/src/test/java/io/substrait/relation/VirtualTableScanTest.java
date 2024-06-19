package io.substrait.relation;

import static io.substrait.expression.ExpressionCreator.list;
import static io.substrait.expression.ExpressionCreator.map;
import static io.substrait.expression.ExpressionCreator.string;
import static io.substrait.expression.ExpressionCreator.struct;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.type.NamedStruct;
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
                        R.list(R.STRING),
                        R.map(R.STRING, R.STRING))))
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

  private Map<Expression.Literal, Expression.Literal> mapOf(
      Expression.Literal key, Expression.Literal value) {
    // Map.of() comes only in Java 9 and the "core" module is on Java 8
    HashMap<Expression.Literal, Expression.Literal> map = new HashMap<>();
    map.put(key, value);
    return map;
  }
}
