package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.expression.FunctionLookup;
import io.substrait.expression.proto.FunctionCollector;
import io.substrait.expression.proto.ImmutableFunctionLookup;
import io.substrait.function.SimpleExtension;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.RelConverter;
import java.io.IOException;
import java.util.Arrays;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

public class ProtoRelConverterTest extends PlanTestBase {
  private void assertProtoRelRoundrip(String query) throws IOException, SqlParseException {
    SqlToSubstrait s = new SqlToSubstrait();
    String[] values = asString("tpch/schema.sql").split(";");
    var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();
    Plan p = s.execute(query, creates);
    SimpleExtension.ExtensionCollection extensionCollection = SimpleExtension.loadDefaults();
    FunctionLookup functionLookup = ImmutableFunctionLookup.builder().from(p).build();
    ProtoRelConverter relConverter = new ProtoRelConverter(functionLookup, extensionCollection);
    for (PlanRel planRel : p.getRelationsList()) {
      io.substrait.proto.Rel protoRel1 = planRel.getRoot().getInput();
      Rel rel = relConverter.from(protoRel1);
      io.substrait.proto.Rel protoRel2 = new RelConverter(new FunctionCollector()).toProto(rel);
      assertEquals(protoRel1, protoRel2);
    }
  }

  @Test
  public void aggregate() throws IOException, SqlParseException {
    assertProtoRelRoundrip("select count(DISTINCT L_ORDERKEY),sum(L_ORDERKEY) from lineitem");
  }

  @Test
  public void filter() throws IOException, SqlParseException {
    assertProtoRelRoundrip("select L_ORDERKEY from lineitem WHERE L_ORDERKEY + 1 > 10");
  }

  @Test
  public void joinAggSortLimit() throws IOException, SqlParseException {
    assertProtoRelRoundrip(
        "select\n"
            + "  l.l_orderkey,\n"
            + "  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,\n"
            + "  o.o_orderdate,\n"
            + "  o.o_shippriority\n"
            + "\n"
            + "from\n"
            + "  \"customer\" c,\n"
            + "  \"orders\" o,\n"
            + "  \"lineitem\" l\n"
            + "\n"
            + "where\n"
            + "  c.c_mktsegment = 'HOUSEHOLD'\n"
            + "  and c.c_custkey = o.o_custkey\n"
            + "  and l.l_orderkey = o.o_orderkey\n"
            + "  and o.o_orderdate < date '1995-03-25'\n"
            + "  and l.l_shipdate > date '1995-03-25'\n"
            + "\n"
            + "group by\n"
            + "  l.l_orderkey,\n"
            + "  o.o_orderdate,\n"
            + "  o.o_shippriority\n"
            + "order by\n"
            + "  revenue desc,\n"
            + "  o.o_orderdate\n"
            + "limit 10");
  }
}
