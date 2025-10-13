package io.substrait.spark

import io.substrait.spark.logical.{ToLogicalPlan, ToSubstraitRel}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import io.substrait.extension.ExtensionLookup
import io.substrait.plan.{PlanProtoConverter, ProtoPlanConverter}
import io.substrait.relation.ProtoRelConverter

class HiveTableSuite extends SparkFunSuite {
  var spark: SparkSession =
    SparkSession.builder().config("spark.master", "local").enableHiveSupport().getOrCreate()

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel("WARN")

    // introduced in spark 3.4
    spark.conf.set("spark.sql.readSideCharPadding", "false")
    spark.conf.set("spark.sql.legacy.createHiveTableByDefault", "false")
  }

  def assertRoundTripExtension(plan: LogicalPlan): LogicalPlan = {
    val toSubstrait = new ToSubstraitRel
    val substraitPlan = toSubstrait.convert(plan)

    // Serialize to proto buffer
    val bytes = new PlanProtoConverter()
      .toProto(substraitPlan)
      .toByteArray

    // Read it back
    val protoPlan = io.substrait.proto.Plan
      .parseFrom(bytes)
    val converter = new ProtoPlanConverter {
      override protected def getProtoRelConverter(
          functionLookup: ExtensionLookup): ProtoRelConverter = {
        new FileHolderHandlingProtoRelConverter(functionLookup)
      }
    }
    val substraitPlan2 = converter.from(protoPlan)
    val sparkPlan2 = new ToLogicalPlan(spark).convert(substraitPlan2)
    val roundTrippedPlan = toSubstrait.convert(sparkPlan2)
    assertResult(substraitPlan)(roundTrippedPlan)

    sparkPlan2
  }

  test("Create / Drop table") {
    spark.sql("drop table if exists cdtest")

    val create = spark.sql("create table cdtest(ID int, VALUE string) using hive")
    assertResult(true)(spark.catalog.tableExists("cdtest"))

    val drop = spark.sql("drop table cdtest")
    assertResult(false)(spark.catalog.tableExists("cdtest"))

    // convert the plans to Substrait and back
    val cPlan = assertRoundTripExtension(create.queryExecution.optimizedPlan)
    val dPlan = assertRoundTripExtension(drop.queryExecution.optimizedPlan)

    // execute the round-tripped 'create' plan and assert the table exists
    spark.sessionState.executePlan(cPlan).executedPlan.execute()
    assertResult(true)(spark.catalog.tableExists("cdtest"))

    // execute the round-tripped 'drop' plan and assert the table no longer exists
    spark.sessionState.executePlan(dPlan).executedPlan.execute()
    assertResult(false)(spark.catalog.tableExists("cdtest"))
  }

  test("Insert into Hive table") {
    // create a Hive table with 2 rows
    spark.sql("drop table if exists test")
    spark.sql("create table test(ID int, VALUE string) using hive")
    spark.sql("insert into test values(1001, 'hello')")
    spark.sql("insert into test values(1002, 'world')")
    assertResult(2)(spark.sql("select * from test").count())

    // insert a new row - and capture the query plan
    val insert = spark.sql("insert into test values(1003, 'again')")
    // there are now 3 rows
    assertResult(3)(spark.sql("select * from test").count())

    // convert the plan to Substrait and back
    val plan = assertRoundTripExtension(insert.queryExecution.optimizedPlan)
    // this should not have affected the table (still 3 rows)
    assertResult(3)(spark.sql("select * from test").count())

    // now execute the round-tripped plan and assert an extra row is appended
    spark.sessionState.executePlan(plan).executedPlan.execute()
    assertResult(4)(spark.sql("select * from test").count())
    // and again...
    spark.sessionState.executePlan(plan).executedPlan.execute()
    assertResult(5)(spark.sql("select * from test").count())
    // check it has inserted the correct values
    assertResult(3)(spark.sql("select * from test where ID = 1003 and VALUE = 'again'").count())
  }

  test("Create Table As Select") {
    spark.sql("drop table if exists ctas")
    val df = spark.sql(
      "create table ctas using hive as select * from (values (1, 'a'), (2, 'b') as table(col1, col2))")
    assertResult(2)(spark.sql("select * from ctas").count())

    spark.sql("drop table ctas")

    val plan = assertRoundTripExtension(df.queryExecution.optimizedPlan)
    spark.sessionState.executePlan(plan).executedPlan.execute()
    assertResult(2)(spark.sql("select * from ctas").count())
  }

}
