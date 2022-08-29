package io.substrait.spark.iceberg;

import io.substrait.spark.BaseSparkSqlPlanTest;
import io.substrait.spark.SparkLogicalPlanConverter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class IcebergSparkSqlPlanTest extends BaseSparkSqlPlanTest {

  public static void prepareSparkTables(SparkSession spark) throws IOException {
    spark.sql("use local");
    spark.sql("CREATE DATABASE IF NOT EXISTS tpch");
    spark.sql("use tpch");
    String tpchCreateTableString =
        FileUtils.readFileToString(
            new File("src/test/resources/iceberg_tpch_schema.sql"), StandardCharsets.UTF_8);
    Arrays.stream(tpchCreateTableString.split(";"))
        .filter(StringUtils::isNotBlank)
        .toList()
        .forEach(spark::sql);
    spark.sql("show tables").show();
  }

  @BeforeAll
  public static void beforeAll() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config(
                "spark.sql.catalog.local.warehouse",
                new File("iceberg-warehouse").getAbsolutePath())
            .getOrCreate();
    try {
      prepareSparkTables(spark);
    } catch (IOException e) {
      Assertions.fail(e);
    }
  }

  @Test
  public void testProject() {
    LogicalPlan plan =
        plan("select lower(l_comment) from lineitem where length(l_comment)>0 limit 10");
    System.out.println(plan.treeString());
    SparkLogicalPlanConverter.convert(plan);
  }
}
