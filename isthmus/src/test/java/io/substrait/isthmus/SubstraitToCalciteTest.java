package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.plan.Plan;
import io.substrait.plan.ProtoPlanConverter;
import java.util.Map.Entry;
import org.apache.calcite.adapter.tpcds.TpcdsSchema;
import org.apache.calcite.rel.RelRoot;
import org.junit.jupiter.api.Test;

public class SubstraitToCalciteTest extends PlanTestBase {
  @Test
  void testConvertRoot() throws Exception {
    SqlToSubstrait s = new SqlToSubstrait();
    TpcdsSchema schema = new TpcdsSchema(1.0);

    // single column
    String newColName = "store_name";
    String sql = "select s_store_name as " + newColName + " from tpcds.store";

    io.substrait.proto.Plan protoPlan = s.execute(sql, "tpcds", schema);

    ProtoPlanConverter protoPlanConverter = new ProtoPlanConverter(extensions);
    Plan plan = protoPlanConverter.from(protoPlan);
    Plan.Root root = plan.getRoots().get(0);

    assertEquals(1, root.getNames().size());
    assertEquals(newColName.toUpperCase(), root.getNames().get(0));

    SubstraitToCalcite converter = new SubstraitToCalcite(extensions, typeFactory);
    RelRoot relRoot = converter.convert(root);

    assertEquals(root.getNames().size(), relRoot.fields.size());
    for (Entry<Integer, String> field : relRoot.fields) {
      assertEquals(root.getNames().get(field.getKey()), field.getValue());
    }

    // multiple columns
    String storeIdColumnName = "s_store_id";
    sql = "select " + storeIdColumnName + ", s_store_name as " + newColName + " from tpcds.store";
    protoPlan = s.execute(sql, "tpcds", schema);

    protoPlanConverter = new ProtoPlanConverter(extensions);
    plan = protoPlanConverter.from(protoPlan);
    root = plan.getRoots().get(0);

    assertEquals(2, root.getNames().size());
    assertEquals(storeIdColumnName.toUpperCase(), root.getNames().get(0));
    assertEquals(newColName.toUpperCase(), root.getNames().get(1));

    relRoot = converter.convert(root);

    assertEquals(root.getNames().size(), relRoot.fields.size());
    for (Entry<Integer, String> field : relRoot.fields) {
      assertEquals(root.getNames().get(field.getKey()), field.getValue());
    }
  }
}
