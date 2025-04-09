package io.substrait.examples;

import static io.substrait.examples.SparkHelper.ROOT_DIR;
import static io.substrait.examples.SparkHelper.TESTS_CSV;
import static io.substrait.examples.SparkHelper.VEHICLES_CSV;

import io.substrait.examples.util.SubstraitStringify;
import io.substrait.spark.logical.ToSubstraitRel;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/** Minimal Spark application */
public class SparkDataset implements App.Action {

  @Override
  public void run(String arg) {

    // Connect to a local in-process Spark instance
    try (SparkSession spark = SparkHelper.connectLocalSpark()) {

      Dataset<Row> dsVehicles;
      Dataset<Row> dsTests;

      // load from CSV files
      String vehiclesFile = Paths.get(ROOT_DIR, VEHICLES_CSV).toString();
      String testsFile = Paths.get(ROOT_DIR, TESTS_CSV).toString();

      System.out.println("Reading " + vehiclesFile);
      System.out.println("Reading " + testsFile);

      dsVehicles = spark.read().option("delimiter", ",").option("header", "true").csv(vehiclesFile);
      dsVehicles.show();

      dsTests = spark.read().option("delimiter", ",").option("header", "true").csv(testsFile);
      dsTests.show();

      // created the joined dataset
      Dataset<Row> joinedDs =
          dsVehicles
              .join(dsTests, dsVehicles.col("vehicle_id").equalTo(dsTests.col("vehicle_id")))
              .filter(dsTests.col("test_result").equalTo("P"))
              .groupBy(dsVehicles.col("colour"))
              .count();

      joinedDs = joinedDs.orderBy(joinedDs.col("count"));
      joinedDs.show();

      LogicalPlan plan = joinedDs.queryExecution().optimizedPlan();

      System.out.println(plan);
      createSubstrait(plan);

      spark.stop();
    } catch (Exception e) {
      e.printStackTrace(System.out);
    }
  }

  /**
   * Create substrait plan and save to file based on logical plan
   *
   * @param enginePlan logical plan
   */
  public void createSubstrait(LogicalPlan enginePlan) {
    ToSubstraitRel toSubstrait = new ToSubstraitRel();
    io.substrait.plan.Plan plan = toSubstrait.convert(enginePlan);

    SubstraitStringify.explain(plan).forEach(System.out::println);

    byte[] buffer = plan.toProto().toByteArray();
    try {
      Files.write(Paths.get(ROOT_DIR, "spark_dataset_substrait.plan"), buffer);
      System.out.println("File written to " + Paths.get(ROOT_DIR, "spark_sql_substrait.plan"));
    } catch (IOException e) {
      e.printStackTrace(System.out);
    }
  }
}
