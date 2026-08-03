package io.substrait.spark.docs

import io.substrait.plan.{PlanProtoConverter, ProtoPlanConverter}
import io.substrait.spark.{SparkExtension, SubstraitPlanTestBase}
import io.substrait.spark.logical.{ToLogicalPlan, ToSubstraitRel}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.classic.DatasetUtil
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Backs the code samples in the Spark docs pages. Regions marked with
 * `// --8<-- [start:name]` / `[end:name]` are pulled into the docs via `--8<--` snippet includes,
 * so the samples shown there are exactly this compiled and executed code. The full
 * produce -> serialize -> consume -> execute round trip runs on every Spark variant in CI.
 */
class DocExamplesSuite extends SparkFunSuite with SharedSparkSession with SubstraitPlanTestBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext.setLogLevel("WARN")
    val session = spark
    import session.implicits._
    Seq((1, "red"), (2, "blue"), (3, "red"))
      .toDF("vehicle_id", "colour")
      .createOrReplaceTempView("vehicles")
    Seq((1, "P"), (2, "F"), (3, "P"))
      .toDF("vehicle_id", "test_result")
      .createOrReplaceTempView("tests")
  }

  test("index entry points") {
    val optimizedPlan = spark.sql("SELECT colour FROM vehicles").queryExecution.optimizedPlan
    // --8<-- [start:index-to-substrait]
    val toSubstrait = new ToSubstraitRel
    val plan = toSubstrait.convert(optimizedPlan)
    // --8<-- [end:index-to-substrait]

    // --8<-- [start:index-to-logical]
    val toSpark = new ToLogicalPlan(spark)
    val sparkPlan = toSpark.convert(plan)
    // --8<-- [end:index-to-logical]
    assert(sparkPlan.resolved)
  }

  test("producing plans (SQL API)") {
    // --8<-- [start:sql-api]
    val result = spark.sql(
      "SELECT vehicles.colour, count(*) AS colourcount" +
        " FROM vehicles" +
        " INNER JOIN tests ON vehicles.vehicle_id = tests.vehicle_id" +
        " WHERE tests.test_result = 'P'" +
        " GROUP BY vehicles.colour" +
        " ORDER BY count(*)")

    val optimised = result.queryExecution.optimizedPlan
    // --8<-- [end:sql-api]

    // --8<-- [start:convert]
    val toSubstrait = new ToSubstraitRel
    val plan = toSubstrait.convert(optimised)
    // --8<-- [end:convert]

    // --8<-- [start:serialize]
    val buffer = new PlanProtoConverter().toProto(plan).toByteArray
    // --8<-- [end:serialize]
    assert(buffer.length > 0)
  }

  test("producing plans (DataFrame API)") {
    val session = spark
    import session.implicits._
    val dsVehicles = Seq((1, "red"), (2, "blue"), (3, "red")).toDF("vehicle_id", "colour")
    val dsTests = Seq((1, "P"), (2, "F"), (3, "P")).toDF("vehicle_id", "test_result")
    // --8<-- [start:dataframe-api]
    val joined = dsVehicles
      .join(dsTests, dsVehicles.col("vehicle_id").equalTo(dsTests.col("vehicle_id")))
      .filter(dsTests.col("test_result").equalTo("P"))
      .groupBy(dsVehicles.col("colour"))
      .count()
      .orderBy("count")

    val optimised = joined.queryExecution.optimizedPlan
    // --8<-- [end:dataframe-api]
    assert(optimised != null)
  }

  test("rdd limit") {
    // --8<-- [start:rdd-limit]
    val toSubstrait = new ToSubstraitRel
    toSubstrait.rddLimit = 1000
    // --8<-- [end:rdd-limit]
    assert(toSubstrait.rddLimit == 1000)
  }

  test("consuming plans") {
    val sourcePlan =
      new ToSubstraitRel().convert(
        spark
          .sql(
            "SELECT vehicles.colour, count(*) AS colourcount" +
              " FROM vehicles" +
              " INNER JOIN tests ON vehicles.vehicle_id = tests.vehicle_id" +
              " WHERE tests.test_result = 'P'" +
              " GROUP BY vehicles.colour" +
              " ORDER BY count(*)")
          .queryExecution
          .optimizedPlan)
    val buffer = new PlanProtoConverter().toProto(sourcePlan).toByteArray

    // --8<-- [start:parse]
    val proto = io.substrait.proto.Plan.parseFrom(buffer)
    // --8<-- [end:parse]

    // --8<-- [start:to-pojo]
    val plan = new ProtoPlanConverter(SparkExtension.COLLECTION).from(proto)
    // --8<-- [end:to-pojo]

    // --8<-- [start:rebuild]
    val toSpark = new ToLogicalPlan(spark)
    val sparkPlan = toSpark.convert(plan)
    // --8<-- [end:rebuild]

    // Execute the rebuilt plan to prove the round trip runs. The docs show the public
    // `Dataset.ofRows(spark, sparkPlan).show()`; DatasetUtil is a version-neutral test shim.
    assert(DatasetUtil.fromLogicalPlan(spark, sparkPlan).collect().length >= 0)
  }
}
