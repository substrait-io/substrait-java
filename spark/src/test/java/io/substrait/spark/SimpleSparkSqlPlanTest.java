package io.substrait.spark;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.Test;

public class SimpleSparkSqlPlanTest extends BaseSparkSqlPlanTest {

  @Test
  public void testSimpleProject() {
    LogicalPlan plan = plan("select lower(l_comment) from lineitem");
    System.out.println(plan.treeString());
    SparkLogicalPlanConverter.convert(plan);
  }
}
