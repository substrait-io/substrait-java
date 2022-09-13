package io.substrait.spark;

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

public class SparkSqlPlanTest extends BaseSparkSqlPlanTest {

  public static void prepareSparkTables(SparkSession spark) throws IOException {
    File localWareHouseDir = new File("spark-warehouse");
    if (localWareHouseDir.exists()) {
      FileUtils.deleteDirectory(localWareHouseDir);
    }
    FileUtils.forceMkdir(localWareHouseDir);
    spark.sql("DROP DATABASE IF EXISTS tpch CASCADE");
    spark.sql("CREATE DATABASE IF NOT EXISTS tpch");
    spark.sql("use tpch");
    String tpchCreateTableString =
        FileUtils.readFileToString(
            new File("src/test/resources/tpch_schema.sql"), StandardCharsets.UTF_8);
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
            .config("spark.sql.legacy.createHiveTableByDefault", "false")
            .getOrCreate();
    try {
      prepareSparkTables(spark);
    } catch (IOException e) {
      Assertions.fail(e);
    }
  }

  @Test
  public void testReadRel() {
    LogicalPlan plan = plan("select * from lineitem");
    System.out.println(plan.treeString());
    System.out.println(SparkLogicalPlanConverter.convert(plan));
  }
}
