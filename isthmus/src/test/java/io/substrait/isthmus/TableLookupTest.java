package io.substrait.isthmus;

import static io.substrait.isthmus.SqlConverterBase.EXTENSION_COLLECTION;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.substrait.relation.Rel;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TableLookupTest extends PlanTestBase {

  private static Map<String, Type> lineitem =
      ImmutableMap.<String, Type>builder()
          .put("L_ORDERKEY", TypeCreator.REQUIRED.I64)
          .put("L_PARTKEY", TypeCreator.REQUIRED.I64)
          .put("L_SUPPKEY", TypeCreator.REQUIRED.I64)
          .put("L_LINENUMBER", TypeCreator.NULLABLE.I32)
          .put("L_QUANTITY", TypeCreator.NULLABLE.decimal(38, 0))
          .put("L_EXTENDEDPRICE", TypeCreator.NULLABLE.decimal(38, 0))
          .put("L_DISCOUNT", TypeCreator.NULLABLE.decimal(38, 0))
          .put("L_TAX", TypeCreator.NULLABLE.decimal(38, 0))
          .put("L_RETURNFLAG", TypeCreator.NULLABLE.fixedChar(1))
          .put("L_LINESTATUS", TypeCreator.NULLABLE.fixedChar(1))
          .put("L_SHIPDATE", TypeCreator.NULLABLE.DATE)
          .put("L_COMMITDATE", TypeCreator.NULLABLE.DATE)
          .put("L_RECEIPTDATE", TypeCreator.NULLABLE.DATE)
          .put("L_SHIPINSTRUCT", TypeCreator.NULLABLE.fixedChar(25))
          .put("L_SHIPMODE", TypeCreator.NULLABLE.fixedChar(10))
          .put("L_COMMENT", TypeCreator.NULLABLE.varChar(44))
          .build();

  @Test
  void compareProto() throws SqlParseException, IOException {
    var names = Lists.<String>newArrayList();
    var types = Lists.<Type>newArrayList();
    lineitem.forEach(
        (k, v) -> {
          names.add(k);
          types.add(v);
        });
    var struct = NamedStruct.of(names, Type.Struct.builder().fields(types).nullable(false).build());
    SqlToSubstrait s2s1 = new SqlToSubstrait();
    SqlToSubstrait s2s2 = new SqlToSubstrait();
    String[] values = asString("tpch/schema.sql").split(";");
    var creates =
        Arrays.stream(values)
            .filter(t -> !t.trim().isBlank())
            .collect(java.util.stream.Collectors.toList());
    String query = asString("tpch/queries/01.sql");
    var plan1 = s2s1.execute(query, creates);
    var plan2 =
        s2s2.execute(
            query,
            (tn) -> {
              if (tn.size() == 1 && Objects.equals(tn.get(0), "LINEITEM")) {
                return struct;
              }
              return null;
            });
    System.out.println(JsonFormat.printer().includingDefaultValueFields().print(plan1));
    System.out.println(JsonFormat.printer().includingDefaultValueFields().print(plan2));
    Assertions.assertEquals(plan1, plan2);
  }

  @Test
  void testNamespaced() throws SqlParseException, InvalidProtocolBufferException {
    var names = Lists.<String>newArrayList();
    var types = Lists.<Type>newArrayList();
    lineitem.forEach(
        (k, v) -> {
          names.add(k);
          types.add(v);
        });
    var struct = NamedStruct.of(names, Type.Struct.builder().fields(types).nullable(false).build());
    SqlToSubstrait s = new SqlToSubstrait();
    var plan =
        s.execute(
            "SELECT * from foobar.tpch.lineitem",
            (tn) -> {
              if (tn.size() == 3 && Objects.equals(tn.get(2), "LINEITEM")) {
                return struct;
              }
              return null;
            });
    System.out.println(JsonFormat.printer().includingDefaultValueFields().print(plan));
  }

  @Test
  void testNestedStruct() throws SqlParseException, InvalidProtocolBufferException {
    var names = Lists.newArrayList("a", "b", "c", "d");
    var types =
        Lists.newArrayList(
            TypeCreator.NULLABLE.I64,
            TypeCreator.NULLABLE.struct(TypeCreator.NULLABLE.I64, TypeCreator.NULLABLE.FP64));
    var struct = NamedStruct.of(names, Type.Struct.builder().fields(types).nullable(false).build());
    SqlToSubstrait s = new SqlToSubstrait();
    var plan =
        s.execute(
            "SELECT * from lineitem",
            (tn) -> {
              if (tn.size() == 1 && Objects.equals(tn.get(0), "LINEITEM")) {
                return struct;
              }
              return null;
            });
    System.out.println(JsonFormat.printer().includingDefaultValueFields().print(plan));
  }

  @Test
  void testSubstrait2SqlWithTableLookup() throws SqlParseException, InvalidProtocolBufferException {
    var names = Lists.<String>newArrayList();
    var types = Lists.<Type>newArrayList();
    lineitem.forEach(
        (k, v) -> {
          names.add(k);
          types.add(v);
        });
    var struct = NamedStruct.of(names, Type.Struct.builder().fields(types).nullable(false).build());
    Function<List<String>, NamedStruct> tableLookup =
        (tn) -> {
          if (tn.size() == 3 && Objects.equals(tn.get(2), "LINEITEM")) {
            return struct;
          }
          return null;
        };

    test("SELECT l_partkey, l_orderkey from foobar.tpch.lineitem", tableLookup);
  }

  private void test(String sql, Function<List<String>, NamedStruct> tableLookup)
      throws SqlParseException {
    // 1. sql -> substrait rel
    SqlToSubstrait s = new SqlToSubstrait();
    for (RelRoot relRoot : s.sqlToRelNode(sql, tableLookup)) {
      Rel pojoRel = SubstraitRelVisitor.convert(relRoot, EXTENSION_COLLECTION);

      // 2. substrait rel -> Calcite Rel
      RelNode relnodeRoot = new SubstraitToSql().substraitRelToCalciteRel(pojoRel, tableLookup);

      // 3. Calcite Rel -> substrait rel
      Rel pojoRel2 =
          SubstraitRelVisitor.convert(
              RelRoot.of(relnodeRoot, SqlKind.SELECT), EXTENSION_COLLECTION);

      Assertions.assertEquals(pojoRel, pojoRel2);
      // 4. Calcite Rel -> sql
      String convertedSql = SubstraitToSql.toSql(relnodeRoot);
      System.out.println(String.format("Converted SQL:\n%s", convertedSql));
    }
  }
}
