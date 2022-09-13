package io.substrait.spark.iceberg;

import io.substrait.spark.BaseSparkSqlPlanTest;
import io.substrait.spark.SparkLogicalPlanConverter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Consumer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;

public class IcebergSparkSqlPlanTest extends BaseSparkSqlPlanTest {

  static final boolean IS_START_HIVE_METASTORE_CONTAINER = true;

  static DockerComposeContainer HIVE_METASTORE_CONTAINER;

  static String HIVE_METASTORE_URIS = "thrift://localhost:9083";

  static final String WARE_HOUSE_DIR = "/tmp/hive/warehouse";

  static {
    if (IS_START_HIVE_METASTORE_CONTAINER) {
      // for fast development,use cmd ` docker-compose -f
      // spark/src/test/resources/hive-standalone-metastore/docker-compose.yml up -d `
      Consumer<OutputFrame> consumer =
          new Consumer<OutputFrame>() {
            @Override
            public void accept(OutputFrame outputFrame) {
              String utf8String = outputFrame.getUtf8String();
              System.out.print(utf8String);
            }
          };
      HIVE_METASTORE_CONTAINER =
          new DockerComposeContainer(
                  new File("src/test/resources/hive-standalone-metastore/docker-compose.yml"))
              .withLogConsumer("hive-metastore-database", consumer)
              .withLogConsumer("hive-standalone-metastore", consumer)
              .withExposedService("hive-metastore-database", 3306, Wait.forListeningPort())
              .withExposedService("hive-standalone-metastore", 9083, Wait.forListeningPort());
      HIVE_METASTORE_CONTAINER.start();
      int hiveMetaStorePort =
          HIVE_METASTORE_CONTAINER.getServicePort("hive-standalone-metastore", 9083);
      HIVE_METASTORE_URIS = HIVE_METASTORE_URIS.replace("9083", hiveMetaStorePort + "");
    }
  }

  public static void prepareSparkTables(SparkSession spark) throws IOException {
    File localWareHouseDir = new File(WARE_HOUSE_DIR);
    if (localWareHouseDir.exists()) {
      FileUtils.deleteDirectory(localWareHouseDir);
    }
    FileUtils.forceMkdir(localWareHouseDir);
    spark.sql("DROP DATABASE IF EXISTS iceberg_tpch CASCADE");
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg_tpch");
    spark.sql("use iceberg_tpch");
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
            // iceberg config
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.sql.catalog.spark_catalog.uri", HIVE_METASTORE_URIS)

            // hive config
            .config("hive.metastore.uris", HIVE_METASTORE_URIS)
            .config("spark.sql.warehouse.dir", WARE_HOUSE_DIR)
            .enableHiveSupport()
            .getOrCreate();
    try {
      prepareSparkTables(spark);
    } catch (IOException e) {
      Assertions.fail(e);
    }
  }

  @Test
  public void testReadRel() {
    LogicalPlan iceberg_plan = plan("select * from lineitem");
    System.out.println(iceberg_plan.treeString());
    System.out.println(SparkLogicalPlanConverter.convert(iceberg_plan));

    sql("CREATE TABLE hive_lineitem USING parquet as SELECT * FROM lineitem");
    LogicalPlan hive_plan = plan("""
        SELECT * FROM  hive_lineitem
        """);
    System.out.println(hive_plan.treeString());
    System.out.println(SparkLogicalPlanConverter.convert(hive_plan));

    sql("CREATE TABLE hive_non_parquet_lineitem  as SELECT * FROM lineitem");
    LogicalPlan hive_non_parquet_plan =
        plan("""
        SELECT * FROM  hive_non_parquet_lineitem
        """);
    System.out.println(hive_non_parquet_plan.treeString());
    System.out.println(SparkLogicalPlanConverter.convert(hive_non_parquet_plan));

    LogicalPlan parquet_read_plan =
        spark.read().parquet("src/test/resources/local-files/users.parquet").logicalPlan();
    System.out.println(parquet_read_plan.treeString());
  }
}
