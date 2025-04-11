package io.substrait.isthmus;

import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.substrait.plan.ProtoPlanConverter;
import io.substrait.proto.Plan;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Updated TPC-H test to convert SQL to Substrait and replay those plans back to SQL Validating that
 * the conversions can operate without exceptions
 */
@TestMethodOrder(OrderAnnotation.class)
@TestInstance(Lifecycle.PER_CLASS)
public class TestTpchQuery extends PlanTestBase {

  private Map<Integer, Plan> allPlans = new HashMap<>();

  // Keep list of the known test failures
  // The `fromSubstrait` also assumes the to substrait worked as well
  public static final List<Integer> toSubstraitKnownFails = List.of(22);
  public static final List<Integer> fromSubstraitKnownFails = List.of(7, 8, 9);

  @ParameterizedTest
  @Order(1)
  @ValueSource(
      ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22})
  public void tpchToSubstrait(int query) throws Exception {
    assumeFalse(toSubstraitKnownFails.contains(query));

    SqlToSubstrait s = new SqlToSubstrait();
    String[] values = asString("tpch/schema.sql").split(";");
    var creates =
        Arrays.stream(values)
            .filter(t -> !t.trim().isBlank())
            .collect(java.util.stream.Collectors.toList());
    Plan protoPlan = s.execute(asString(String.format("tpch/queries/%02d.sql", query)), creates);

    allPlans.put(query, protoPlan);
  }

  @ParameterizedTest
  @Order(2)
  @ValueSource(
      ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22})
  public void tpchFromSubstrait(int query) throws Exception {
    assumeFalse(fromSubstraitKnownFails.contains(query));
    assumeTrue(allPlans.containsKey(query));

    Plan possible = allPlans.get(query);

    io.substrait.plan.Plan plan = new ProtoPlanConverter().from(possible);
    SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);
    RelNode relRoot = substraitToCalcite.convert(plan.getRoots().get(0)).project(true);
    System.out.println(SubstraitToSql.toSql(relRoot));
  }
}
