package io.substrait.isthmus;

import com.google.protobuf.util.JsonFormat;
import java.util.Arrays;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class KeyConstraintsTest extends PlanTestBase {

  @ParameterizedTest
  @ValueSource(ints = {7})
  public void tpcds(int query) throws Exception {
    SqlToSubstrait s = new SqlToSubstrait();
    String[] values = asString("keyconstraints_schema.sql").split(";");
    var creates =
        Arrays.stream(values)
            .filter(t -> !t.trim().isBlank())
            .collect(java.util.stream.Collectors.toList());
    var plan = s.execute(asString(String.format("tpcds/queries/%02d.sql", query)), creates);
    System.out.println(JsonFormat.printer().print(plan));
  }
}
