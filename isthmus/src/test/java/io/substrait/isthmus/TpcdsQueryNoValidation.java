package io.substrait.isthmus;

import com.google.protobuf.util.JsonFormat;
import java.util.Arrays;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TpcdsQueryNoValidation extends PlanTestBase {

  /**
   * This test only validates that generating substrait plans for TPC-DS queries does not fail. As
   * of now this test does not validate correctness of the generated plan
   */
  @ParameterizedTest
  @ValueSource(ints = {7})
  public void tpcds(int query) throws Exception {
    SqlToSubstrait s = new SqlToSubstrait();
    String[] values = asString("tpcds/schema.sql").split(";");
    var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();
    var plan = s.execute(asString(String.format("tpcds/queries/%02d.sql", query)), creates);
    System.out.println(JsonFormat.printer().print(plan));
  }
}
