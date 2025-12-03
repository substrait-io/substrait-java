package io.substrait.examples;

import static io.substrait.examples.SparkHelper.ROOT_DIR;

import io.substrait.examples.util.SubstraitStringify;
import io.substrait.plan.Plan;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.spark.logical.ToLogicalPlan;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/** Minimal Spark application */
public class SparkConsumeSubstrait implements App.Action {

  @Override
  public void run(final String arg) {

    // Connect to a local in-process Spark instance
    try (SparkSession spark = SparkHelper.connectLocalSpark()) {

      System.out.println("Reading from " + arg);
      final byte[] buffer = Files.readAllBytes(Paths.get(ROOT_DIR, arg));

      final io.substrait.proto.Plan proto = io.substrait.proto.Plan.parseFrom(buffer);
      final ProtoPlanConverter protoToPlan = new ProtoPlanConverter();
      final Plan plan = protoToPlan.from(proto);

      SubstraitStringify.explain(plan).forEach(System.out::println);

      final ToLogicalPlan substraitConverter = new ToLogicalPlan(spark);
      final LogicalPlan sparkPlan = substraitConverter.convert(plan);

      System.out.println(sparkPlan);

      Dataset.ofRows(spark, sparkPlan).show();

      spark.stop();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
