package io.substrait.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.AfterAll;

public class BaseSparkSqlPlanTest {

  protected static SparkSession spark;

  @AfterAll
  public static void afterAll() {
    if (spark != null) {
      spark.stop();
    }
  }

  protected static Dataset<Row> sql(String sql) {
    System.out.println(sql);
    return spark.sql(sql);
  }

  protected static LogicalPlan plan(String sql) {
    return sql(sql).queryExecution().optimizedPlan();
  }
}
