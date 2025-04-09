package io.substrait.isthmus;

import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.substrait.plan.ProtoPlanConverter;
import io.substrait.proto.Plan;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.adapter.tpcds.TpcdsSchema;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.BeforeAll;
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
public class TestTpcdsQuery extends PlanTestBase {

  private List<Optional<Plan>> allPlans;

  @BeforeAll
  public void setup() {
    allPlans = new ArrayList<Optional<Plan>>();
    for (int i = 1; i < 101; i++) {
      allPlans.add(Optional.empty());
    }
  }

  // Keep list of the known test failures
  // The `fromSubstrait` also assumes the to substrait worked as well
  public static final List<Integer> toSubstraitKnownFails =
      List.of(5, 9, 12, 20, 27, 36, 47, 51, 53, 57, 63, 66, 70, 80, 84, 86, 89, 91, 98);
  public static final List<Integer> fromSubstraitKnownFails = List.of(1, 8, 30, 49, 67, 81);

  @ParameterizedTest
  @Order(1)
  @ValueSource(
      ints = {
        1, 3, 4, 6, 7, 8, 10, 11, 13, 14, 15, 16, 17, 18, 19, 21, 22, 23, 24, 25, 26, 28, 29, 30,
        31, 32, 33, 34, 35, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 48, 49, 50, 52, 54, 55, 56, 58,
        59, 60, 61, 62, 64, 65, 67, 68, 69, 71, 72, 73, 74, 75, 76, 77, 78, 79, 81, 82, 83, 85, 87,
        88, 90, 92, 93, 94, 95, 96, 97, 99, 2, 5, 9, 12, 20, 27, 36, 47, 51, 53, 57, 63, 66, 70, 80,
        84, 86, 89, 91, 98,
      })
  public void tpcdsSuccess(int query) throws Exception {
    assumeFalse(toSubstraitKnownFails.contains(query));

    SqlToSubstrait s = new SqlToSubstrait();
    TpcdsSchema schema = new TpcdsSchema(1.0);
    String sql = asString(String.format("tpcds/queries/%02d.sql", query));
    Plan protoPlan = s.execute(sql, "tpcds", schema);
    allPlans.set(query, Optional.of(protoPlan));
  }

  @ParameterizedTest
  @Order(2)
  @ValueSource(
      ints = {
        1, 3, 4, 6, 7, 8, 10, 11, 13, 14, 15, 16, 17, 18, 19, 21, 22, 23, 24, 25, 26, 28, 29, 30,
        31, 32, 33, 34, 35, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 48, 49, 50, 52, 54, 55, 56, 58,
        59, 60, 61, 62, 64, 65, 67, 68, 69, 71, 72, 73, 74, 75, 76, 77, 78, 79, 81, 82, 83, 85, 87,
        88, 90, 92, 93, 94, 95, 96, 97, 99, 2, 5, 9, 12, 20, 27, 36, 47, 51, 53, 57, 63, 66, 70, 80,
        84, 86, 89, 91, 98,
      })
  public void tpcdsFromSubstrait(int query) throws Exception {

    assumeFalse(fromSubstraitKnownFails.contains(query));
    assumeTrue(allPlans.get(query).isPresent());

    Optional<Plan> possible = allPlans.get(query);

    io.substrait.plan.Plan plan = new ProtoPlanConverter().from(possible.get());
    SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);
    RelNode relRoot = substraitToCalcite.convert(plan.getRoots().get(0)).project(true);
    System.out.println(SubstraitToSql.toSql(relRoot));
  }
}
