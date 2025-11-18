package io.substrait.relation;

import static io.substrait.expression.ExpressionCreator.list;
import static io.substrait.expression.ExpressionCreator.map;
import static io.substrait.expression.ExpressionCreator.string;
import static io.substrait.expression.ExpressionCreator.struct;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Type;
import io.substrait.type.NamedStruct;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class VirtualTableScanTest extends TestBase {

  io.substrait.proto.Expression expression =
      io.substrait.proto.Expression.newBuilder()
          .setLiteral(io.substrait.proto.Expression.Literal.newBuilder().setI32(3))
          .build();

  io.substrait.proto.Expression.Literal literal =
      io.substrait.proto.Expression.Literal.newBuilder().setI32(3).build();

  Type type =
      Type.newBuilder()
          .setI32(
              Type.I32.newBuilder().setNullability(Type.Nullability.NULLABILITY_NULLABLE).build())
          .build();

  io.substrait.proto.NamedStruct schema =
      io.substrait.proto.NamedStruct.newBuilder()
          .setStruct(Type.Struct.newBuilder().addTypes(type).build())
          .addNames("col1")
          .build();

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
                ExpressionCreator.nestedStruct(
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
  void valuesAndFieldsComparisonTest() {
    io.substrait.proto.Rel valuesStructProto = protoRelVirtualTableValues();
    io.substrait.proto.Rel fieldsStructProto = protoRelVirtualTableFields();
    Rel relWithValues = protoRelConverter.from(valuesStructProto);
    Rel relWithFields = protoRelConverter.from(fieldsStructProto);
    assertEquals(relWithValues, relWithFields);
  }

  @Test
  void setUsingOnlyValuesOrFieldsTest() {
    io.substrait.proto.Expression.Literal.Struct literalStruct =
        io.substrait.proto.Expression.Literal.Struct.newBuilder().addFields(literal).build();
    io.substrait.proto.Expression.Nested.Struct nestedStruct =
        io.substrait.proto.Expression.Nested.Struct.newBuilder().addFields(expression).build();

    io.substrait.proto.ReadRel readRel =
        ReadRel.newBuilder()
            .setVirtualTable(
                ReadRel.VirtualTable.newBuilder()
                    .addExpressions(nestedStruct)
                    .addValues(literalStruct)
                    .build())
            .setBaseSchema(schema)
            .build();

    io.substrait.proto.Rel valuesAndFieldsProto =
        io.substrait.proto.Rel.newBuilder().setRead(readRel).build();
    assertThrows(
        IllegalArgumentException.class, () -> protoRelConverter.from(valuesAndFieldsProto));
  }

  io.substrait.proto.Rel protoRelVirtualTableFields() {
    io.substrait.proto.Expression.Nested.Struct struct =
        io.substrait.proto.Expression.Nested.Struct.newBuilder().addFields(expression).build();

    io.substrait.proto.ReadRel readRel =
        ReadRel.newBuilder()
            .setVirtualTable(ReadRel.VirtualTable.newBuilder().addExpressions(struct).build())
            .setBaseSchema(schema)
            .build();

    return io.substrait.proto.Rel.newBuilder().setRead(readRel).build();
  }

  io.substrait.proto.Rel protoRelVirtualTableValues() {
    io.substrait.proto.Expression.Literal.Struct struct =
        io.substrait.proto.Expression.Literal.Struct.newBuilder().addFields(literal).build();

    io.substrait.proto.ReadRel readRel =
        ReadRel.newBuilder()
            .setVirtualTable(ReadRel.VirtualTable.newBuilder().addValues(struct).build())
            .setBaseSchema(schema)
            .build();

    return io.substrait.proto.Rel.newBuilder().setRead(readRel).build();
  }

  @Test
  void notNullableVirtualTable() {
    ImmutableVirtualTableScan.Builder bldr =
        VirtualTableScan.builder()
            .initialSchema(
                NamedStruct.of(Stream.of("column1").collect(Collectors.toList()), R.struct(R.I64)))
            .addRows(
                Expression.NestedStruct.builder()
                    .addFields(ExpressionCreator.i64(true, 1))
                    .nullable(true) // can't have nullable rows
                    .build());
    assertThrows(AssertionError.class, bldr::build);
  }

  private Map<Expression.Literal, Expression.Literal> mapOf(
      Expression.Literal key, Expression.Literal value) {
    // Map.of() comes only in Java 9 and the "core" module is on Java 8
    HashMap<Expression.Literal, Expression.Literal> map = new HashMap<>();
    map.put(key, value);
    return map;
  }
}
