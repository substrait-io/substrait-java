package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.substrait.plan.ProtoPlanConverter;
import io.substrait.proto.Plan;

@TestMethodOrder(OrderAnnotation.class)
@TestInstance(Lifecycle.PER_CLASS)
public class TestTpchQuery extends PlanTestBase {

  private List<Optional<Plan>> allPlans;

  @BeforeAll
  public void setup() {
    allPlans = new ArrayList<Optional<Plan>>();
    for (int i = 1; i < 23; i++) {
      allPlans.add(Optional.empty());
    }
  }

  @ParameterizedTest
  @Order(1)
  @ValueSource(ints = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22 })
  public void tpchToSubstrait(int query) throws Exception {
    SqlToSubstrait s = new SqlToSubstrait();
    String[] values = asString("tpch/schema.sql").split(";");
    var creates = Arrays.stream(values)
        .filter(t -> !t.trim().isBlank())
        .collect(java.util.stream.Collectors.toList());
    Plan protoPlan = s.execute(asString(String.format("tpch/queries/%02d.sql", query)), creates);

    allPlans.set(query, Optional.of(protoPlan));

  }

  @ParameterizedTest
  @Order(2)
  @ValueSource(ints = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22 })
  public void tpchFromSubstrait(int query) throws Exception {
    Optional<Plan> possible = allPlans.get(query);
    if (possible.isPresent()){
      io.substrait.plan.Plan plan = new ProtoPlanConverter().from(possible.get());
      SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);
      RelNode relRoot = substraitToCalcite.convert(plan.getRoots().get(0)).project(true);
      System.out.println(SubstraitToSql.toSql(relRoot));
    } else {

      fail("Unable to convert to SQL");
    }


  }
}
