package io.substrait.isthmus;

import com.google.common.collect.ImmutableMap;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
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
    SqlToSubstrait s2s1 = new SqlToSubstrait();
    SqlToSubstrait s2s2 = new SqlToSubstrait();
    String[] values = asString("tpch/schema.sql").split(";");
    var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();
    String query = asString("tpch/queries/01.sql");
    var plan1 = s2s1.execute(query, creates);
    var plan2 =
        s2s2.execute(
            query,
            (tn) -> {
              if (tn.size() == 1 && Objects.equals(tn.get(0), "LINEITEM")) {
                return lineitem;
              }
              return null;
            });
    Assertions.assertEquals(plan1, plan2);
  }
}
