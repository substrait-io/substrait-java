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

public class NestedStructQueryTest extends PlanTestBase {
  private class TypeHelper {
    private final RelDataTypeFactory factory;

    public TypeHelper(RelDataTypeFactory factory) {
      this.factory = factory;
    }

    RelDataType struct(String field, RelDataType value) {
      return factory.createStructType(Arrays.asList(Pair.of(field, value)));
    }

    RelDataType struct2(String field1, RelDataType value1, String field2, RelDataType value2) {
      return factory.createStructType(
          Arrays.asList(Pair.of(field1, value1), Pair.of(field2, value2)));
    }

    RelDataType i32() {
      return factory.createSqlType(SqlTypeName.INTEGER);
    }

    RelDataType string() {
      return factory.createSqlType(SqlTypeName.VARCHAR);
    }

    RelDataType list(RelDataType elementType) {
      return factory.createArrayType(elementType, -1);
    }

    RelDataType map(RelDataType key, RelDataType value) {
      return factory.createMapType(key, value);
    }
  }

  private void test(Table table, String query, String expectedExpressionText)
      throws SqlParseException, IOException {
    final Schema schema = new SubstraitSchema(Map.of("my_table", table));
    final CalciteCatalogReader catalog = schemaToCatalog("nested", schema);
    final SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
    Plan plan = sqlToSubstrait.execute(query, catalog);
    Expression obtainedExpression =
        plan.getRelations(0).getRoot().getInput().getProject().getExpressions(0);
    Expression expectedExpression = TextFormat.parse(expectedExpressionText, Expression.class);
    assertEquals(expectedExpression, obtainedExpression);

    ProtoPlanConverter converter = new ProtoPlanConverter();
    io.substrait.plan.Plan plan2 = converter.from(plan);
    assertPlanRoundtrip(plan2);
  }

  @Test
  public void testNestedStruct() throws SqlParseException, IOException {
    final Table table =
        new AbstractTable() {
          @Override
          public RelDataType getRowType(RelDataTypeFactory factory) {
            var helper = new TypeHelper(factory);
            return helper.struct2(
                "x", helper.i32(),
                "a", helper.i32());
          }
        };

    String query =
        """
           SELECT
             "nested"."my_table"."a"
           FROM
             "nested"."my_table";
           """;

    String expectedExpressionText =
        """
          selection {
            direct_reference {
              struct_field {
                field: 1 # a
              }
            }
            root_reference: {}
          }
        """;

    test(table, query, expectedExpressionText);
  }

  @Test
  public void testNestedStruct2() throws SqlParseException, IOException {
    final Table table =
        new AbstractTable() {
          @Override
          public RelDataType getRowType(RelDataTypeFactory factory) {
            var helper = new TypeHelper(factory);
            return helper.struct2(
                "x", helper.i32(),
                "a", helper.struct("b", helper.i32()));
          }
        };

    String query =
        """
           SELECT
             "nested"."my_table"."a"."b"
           FROM
             "nested"."my_table";
           """;

    String expectedExpressionText =
        """
          selection {
            direct_reference {
              struct_field {
                field: 1 # a
                child {
                  struct_field {
                    field: 0 # b
                  }
                }
              }
            }
            root_reference: {}
          }
        """;

    test(table, query, expectedExpressionText);
  }

  @Test
  public void testNestedStruct3() throws SqlParseException, IOException {
    final Table table =
        new AbstractTable() {
          @Override
          public RelDataType getRowType(RelDataTypeFactory factory) {
            var helper = new TypeHelper(factory);
            return helper.struct2(
                "aa", helper.i32(),
                "a", helper.struct("b", helper.struct("c", helper.i32())));
          }
        };

    String query =
        """
           SELECT
             "nested"."my_table"."a"."b"."c"
           FROM
             "nested"."my_table";
           """;

    String expectedExpressionText =
        """
          selection {
            direct_reference {
              struct_field {
                field: 1 # a
                child {
                  struct_field {
                    field: 0 # b
                    child: {
                      struct_field {
                        field: 0 # c
                      }
                    }
                  }
                }
              }
            }
            root_reference: {}
          }
        """;

    test(table, query, expectedExpressionText);
  }

  @Test
  public void testNestedList() throws SqlParseException, IOException {
    final Table table =
        new AbstractTable() {
          @Override
          public RelDataType getRowType(RelDataTypeFactory factory) {
            var helper = new TypeHelper(factory);

            return helper.struct2("x", helper.i32(), "a", helper.list(helper.i32()));
          }
        };

    String query =
        """
           SELECT
             "nested"."my_table"."a"[1]
           FROM
             "nested"."my_table";
           """;

    String expectedExpressionText =
        """
            selection {
              direct_reference {
                struct_field {
                  field: 1 # a
                  child {
                    list_element {
                      offset: 1
                    }
                  }
                }
              }
              root_reference: {}
            }
        """;

    test(table, query, expectedExpressionText);
  }

  @Test
  public void testNestedList2() throws SqlParseException, IOException {
    final Table table =
        new AbstractTable() {
          @Override
          public RelDataType getRowType(RelDataTypeFactory factory) {
            var helper = new TypeHelper(factory);

            return helper.struct2(
                "x",
                helper.i32(),
                "a",
                helper.list(helper.list(helper.list(helper.list(helper.i32())))));
          }
        };

    String query =
        """
           SELECT
             "nested"."my_table"."a"[1][2][3]
           FROM
             "nested"."my_table";
           """;

    String expectedExpressionText =
        """
        selection {
          direct_reference {
            struct_field {
              field: 1 # a
              child {
                list_element {
                  offset: 1
                  child {
                    list_element {
                      offset: 2
                      child {
                        list_element {
                          offset: 3
                        }
                      }
                    }
                  }
                }
              }
            }
          }
          root_reference: {}
        }
        """;

    test(table, query, expectedExpressionText);
  }

  @Test
  public void testProtobufDoc() throws SqlParseException, IOException {

    final Table table =
        new AbstractTable() {
          @Override
          public RelDataType getRowType(RelDataTypeFactory factory) {

            var helper = new TypeHelper(factory);
            return helper.struct(
                "a",
                helper.struct(
                    "b",
                    helper.list(
                        helper.struct(
                            "c", helper.map(helper.string(), helper.struct("x", helper.i32()))))));
          }
        };

    String query =
        """
           SELECT
             "nested"."my_table".a.b[2].c['my_map_key'].x
           FROM
             "nested"."my_table";
           """;

    String expectedExpressionText =
        """
          selection {
            direct_reference {
              struct_field {
                field: 0 # .a
                child {
                  struct_field {
                    field: 0 # .b
                    child {
                      list_element {
                        offset: 2
                        child {
                          struct_field {
                            field: 0 # .c
                            child {
                              map_key {
                                map_key {
                                  string: "my_map_key" # ['my_map_key']
                                }
                                child {
                                  struct_field {
                                    field: 0 # .x
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            root_reference {}
          }
        """;
    test(table, query, expectedExpressionText);
  }
}
