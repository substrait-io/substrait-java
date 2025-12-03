package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.TextFormat;
import io.substrait.isthmus.calcite.SubstraitSchema;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.proto.Expression;
import io.substrait.proto.Plan;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.junit.jupiter.api.Test;

class NestedStructQueryTest extends PlanTestBase {
  private class TypeHelper {
    private final RelDataTypeFactory factory;

    public TypeHelper(final RelDataTypeFactory factory) {
      this.factory = factory;
    }

    RelDataType struct(final String field, final RelDataType value) {
      return factory.createStructType(Arrays.asList(Pair.of(field, value)));
    }

    RelDataType struct2(
        final String field1,
        final RelDataType value1,
        final String field2,
        final RelDataType value2) {
      return factory.createStructType(
          Arrays.asList(Pair.of(field1, value1), Pair.of(field2, value2)));
    }

    RelDataType i32() {
      return factory.createSqlType(SqlTypeName.INTEGER);
    }

    RelDataType string() {
      return factory.createSqlType(SqlTypeName.VARCHAR);
    }

    RelDataType list(final RelDataType elementType) {
      return factory.createArrayType(elementType, -1);
    }

    RelDataType map(final RelDataType key, final RelDataType value) {
      return factory.createMapType(key, value);
    }
  }

  private void test(final Table table, final String query, final String expectedExpressionText)
      throws SqlParseException, IOException {
    final Schema schema = new SubstraitSchema(Map.of("my_table", table));
    final CalciteCatalogReader catalog = schemaToCatalog("nested", schema);
    final SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
    final Plan plan = toProto(sqlToSubstrait.convert(query, catalog));
    final Expression obtainedExpression =
        plan.getRelations(0).getRoot().getInput().getProject().getExpressions(0);
    final Expression expectedExpression =
        TextFormat.parse(expectedExpressionText, Expression.class);
    assertEquals(expectedExpression, obtainedExpression);

    final ProtoPlanConverter converter = new ProtoPlanConverter();
    final io.substrait.plan.Plan plan2 = converter.from(plan);
    assertPlanRoundtrip(plan2);
  }

  @Test
  void testNestedStruct() throws SqlParseException, IOException {
    final Table table =
        new AbstractTable() {
          @Override
          public RelDataType getRowType(final RelDataTypeFactory factory) {
            final TypeHelper helper = new TypeHelper(factory);
            return helper.struct2(
                "x", helper.i32(),
                "a", helper.i32());
          }
        };

    final String query =
        "SELECT\n" + "  \"nested\".\"my_table\".\"a\"\n" + "FROM\n" + "  \"nested\".\"my_table\";";

    final String expectedExpressionText =
        "selection {\n"
            + "  direct_reference {\n"
            + "    struct_field {\n"
            + "      field: 1 # a\n"
            + "    }\n"
            + "  }\n"
            + "  root_reference: {}\n"
            + "}";

    test(table, query, expectedExpressionText);
  }

  @Test
  void testNestedStruct2() throws SqlParseException, IOException {
    final Table table =
        new AbstractTable() {
          @Override
          public RelDataType getRowType(final RelDataTypeFactory factory) {
            final TypeHelper helper = new TypeHelper(factory);
            return helper.struct2(
                "x", helper.i32(),
                "a", helper.struct("b", helper.i32()));
          }
        };

    final String query =
        "SELECT\n"
            + "   \"nested\".\"my_table\".\"a\".\"b\"\n"
            + "FROM\n"
            + "  \"nested\".\"my_table\";";

    final String expectedExpressionText =
        "selection {\n"
            + "  direct_reference {\n"
            + "    struct_field {\n"
            + "      field: 1 # a\n"
            + "      child {\n"
            + "        struct_field {\n"
            + "          field: 0 # b\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "  root_reference: {}\n"
            + "}";

    test(table, query, expectedExpressionText);
  }

  @Test
  void testNestedStruct3() throws SqlParseException, IOException {
    final Table table =
        new AbstractTable() {
          @Override
          public RelDataType getRowType(final RelDataTypeFactory factory) {
            final TypeHelper helper = new TypeHelper(factory);
            return helper.struct2(
                "aa", helper.i32(),
                "a", helper.struct("b", helper.struct("c", helper.i32())));
          }
        };

    final String query =
        "SELECT\n"
            + "  \"nested\".\"my_table\".\"a\".\"b\".\"c\"\n"
            + "FROM\n"
            + "   \"nested\".\"my_table\";";

    final String expectedExpressionText =
        "selection {\n"
            + "  direct_reference {\n"
            + "    struct_field {\n"
            + "      field: 1 # a\n"
            + "      child {\n"
            + "        struct_field {\n"
            + "          field: 0 # b\n"
            + "          child: {\n"
            + "            struct_field {\n"
            + "              field: 0 # c\n"
            + "            }\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "  root_reference: {}\n"
            + "}";

    test(table, query, expectedExpressionText);
  }

  @Test
  void testNestedList() throws SqlParseException, IOException {
    final Table table =
        new AbstractTable() {
          @Override
          public RelDataType getRowType(final RelDataTypeFactory factory) {
            final TypeHelper helper = new TypeHelper(factory);

            return helper.struct2("x", helper.i32(), "a", helper.list(helper.i32()));
          }
        };

    final String query =
        "SELECT\n"
            + "  \"nested\".\"my_table\".\"a\"[1]\n"
            + "FROM\n"
            + "  \"nested\".\"my_table\";";

    final String expectedExpressionText =
        "selection {\n"
            + "  direct_reference {\n"
            + "    struct_field {\n"
            + "      field: 1 # a\n"
            + "      child {\n"
            + "        list_element {\n"
            + "          offset: 1\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "  root_reference: {}\n"
            + "}";

    test(table, query, expectedExpressionText);
  }

  @Test
  void testNestedList2() throws SqlParseException, IOException {
    final Table table =
        new AbstractTable() {
          @Override
          public RelDataType getRowType(final RelDataTypeFactory factory) {
            final TypeHelper helper = new TypeHelper(factory);

            return helper.struct2(
                "x",
                helper.i32(),
                "a",
                helper.list(helper.list(helper.list(helper.list(helper.i32())))));
          }
        };

    final String query =
        "SELECT\n"
            + "  \"nested\".\"my_table\".\"a\"[1][2][3]\n"
            + "FROM\n"
            + "  \"nested\".\"my_table\";";

    final String expectedExpressionText =
        "selection {\n"
            + "  direct_reference {\n"
            + "    struct_field {\n"
            + "      field: 1 # a\n"
            + "      child {\n"
            + "        list_element {\n"
            + "          offset: 1\n"
            + "          child {\n"
            + "            list_element {\n"
            + "              offset: 2\n"
            + "              child {\n"
            + "                list_element {\n"
            + "                  offset: 3\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "  root_reference: {}\n"
            + "}";

    test(table, query, expectedExpressionText);
  }

  @Test
  void testProtobufDoc() throws SqlParseException, IOException {

    final Table table =
        new AbstractTable() {
          @Override
          public RelDataType getRowType(final RelDataTypeFactory factory) {

            final TypeHelper helper = new TypeHelper(factory);
            return helper.struct(
                "a",
                helper.struct(
                    "b",
                    helper.list(
                        helper.struct(
                            "c", helper.map(helper.string(), helper.struct("x", helper.i32()))))));
          }
        };

    final String query =
        "SELECT\n"
            + "  \"nested\".\"my_table\".a.b[2].c['my_map_key'].x\n"
            + "FROM\n"
            + "  \"nested\".\"my_table\";";

    final String expectedExpressionText =
        "  selection {\n"
            + "  direct_reference {\n"
            + "    struct_field {\n"
            + "      field: 0 # .a\n"
            + "      child {\n"
            + "        struct_field {\n"
            + "          field: 0 # .b\n"
            + "          child {\n"
            + "            list_element {\n"
            + "              offset: 2\n"
            + "              child {\n"
            + "                struct_field {\n"
            + "                  field: 0 # .c\n"
            + "                  child {\n"
            + "                    map_key {\n"
            + "                      map_key {\n"
            + "                        string: \"my_map_key\" # ['my_map_key']\n"
            + "                      }\n"
            + "                      child {\n"
            + "                        struct_field {\n"
            + "                          field: 0 # .x\n"
            + "                        }\n"
            + "                      }\n"
            + "                    }\n"
            + "                  }\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "  root_reference {}\n"
            + "}\n";
    test(table, query, expectedExpressionText);
  }
}
