package io.substrait.spark;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class BaseSparkSqlPlanTest {

  protected static SparkSession spark;

  @BeforeAll
  public static void beforeAll() throws IOException {
    spark = SparkSession.builder().master("local[2]")
        .config("spark.sql.legacy.createHiveTableByDefault", "false").getOrCreate();
    spark.sql("CREATE DATABASE IF NOT EXISTS tpch");
    spark.sql("use tpch");
    String tpchCreateTableString = FileUtils.readFileToString(
        new File("src/test/resources/tpch_schema.sql"), StandardCharsets.UTF_8);
    Arrays.stream(tpchCreateTableString.split(";")).filter(StringUtils::isNotBlank).toList()
        .forEach(spark::sql);
    spark.sql("show tables").show();
  }

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
