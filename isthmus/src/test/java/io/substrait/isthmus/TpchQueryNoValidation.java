package io.substrait.isthmus;

import com.google.protobuf.util.JsonFormat;
import java.util.Arrays;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@TestMethodOrder(OrderAnnotation.class)
public class TpchQueryNoValidation extends PlanTestBase {

  @ParameterizedTest
  // @ValueSource(ints = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22})
  @ValueSource(
      ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22})
  public void tpch(int query) throws Exception {
    SqlToSubstrait s = new SqlToSubstrait();
    String[] values = asString("tpch/schema.sql").split(";");
    var creates =
        Arrays.stream(values)
            .filter(t -> !t.trim().isBlank())
            .collect(java.util.stream.Collectors.toList());
    var protoPlan = s.execute(asString(String.format("tpch/queries/%02d.sql", query)), creates);
    System.out.println(JsonFormat.printer().print(protoPlan));
  }
}
