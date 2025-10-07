/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.substrait.spark

import io.substrait.spark.logical.{ToLogicalPlan, ToSubstraitRel}

import org.apache.spark.sql.{Dataset, DatasetUtil, Encoders, Row}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import io.substrait.extension.ExtensionLookup
import io.substrait.plan.{PlanProtoConverter, ProtoPlanConverter}
import io.substrait.relation.ProtoRelConverter

import java.nio.file.Paths

class LocalFiles extends SharedSparkSession {
  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext.setLogLevel("WARN")

    conf.setConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED, false)
    // introduced in spark 3.4
    spark.conf.set("spark.sql.readSideCharPadding", "false")
    spark.conf.set("spark.sql.legacy.createHiveTableByDefault", "false")
  }

  def assertRoundTripData(data: Dataset[Row]): Dataset[Row] = {
    val toSubstrait = new ToSubstraitRel
    val sparkPlan = data.queryExecution.optimizedPlan
    val substraitPlan = toSubstrait.convert(sparkPlan)

    // Serialize to proto buffer
    val bytes = new PlanProtoConverter()
      .toProto(substraitPlan)
      .toByteArray

    // Read it back
    val protoPlan = io.substrait.proto.Plan
      .parseFrom(bytes)
    val substraitPlan2 = new ProtoPlanConverter().from(protoPlan)

    val sparkPlan2 = new ToLogicalPlan().convert(substraitPlan2)
    val result = DatasetUtil.fromLogicalPlan(spark, sparkPlan2)

    assertResult(data.columns)(result.columns)
    assertResult(data.count)(result.count)
    data.collect().zip(result.collect()).foreach {
      case (before, after) => assertResult(before)(after)
    }

    // extra check to assert the query plans round-trip as well
    val roundTrippedPlan = toSubstrait.convert(sparkPlan2)
    assertResult(substraitPlan)(roundTrippedPlan)

    result
  }

  test("CSV with header") {
    val table = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(Paths.get("src/test/resources/dataset-a.csv").toAbsolutePath.toString)

    assertRoundTripData(table)
  }

  test("CSV null value") {
    val table = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .option("nullValue", "seven")
      .csv(Paths.get("src/test/resources/dataset-a.csv").toAbsolutePath.toString)

    val result = assertRoundTripData(table)
    val id = result.filter("isnull(VALUE)").head().get(0)

    assertResult(id)(7)
  }

  test("Pipe delimited values") {
    val schema = StructType(
      StructField("ID", IntegerType, false) ::
        StructField("VALUE", StringType, true) :: Nil)

    val table: Dataset[Row] = spark.read
      .schema(schema)
      .option("delimiter", "|")
      .option("quote", "'")
      .csv(Paths.get("src/test/resources/dataset-a.txt").toAbsolutePath.toString)

    assertRoundTripData(table)
  }

  test("Read csv folder") {
    val table = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(Paths.get("src/test/resources/csv/").toAbsolutePath.toString)

    assertRoundTripData(table)
  }

  test("Read parquet file") {
    val table = spark.read
      .parquet(Paths.get("src/test/resources/dataset-a.parquet").toAbsolutePath.toString)

    assertRoundTripData(table)
  }

  test("Read orc file") {
    val table = spark.read
      .orc(Paths.get("src/test/resources/dataset-a.orc").toAbsolutePath.toString)

    assertRoundTripData(table)
  }

  test("Join tables from different formats") {
    val csv = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(Paths.get("src/test/resources/dataset-a.csv").toAbsolutePath.toString)

    val orc = spark.read
      .orc(Paths.get("src/test/resources/dataset-a.orc").toAbsolutePath.toString)
      .withColumnRenamed("ID", "ID_B")
      .withColumnRenamed("VALUE", "VALUE_B");

    val both = csv
      .join(orc, csv.col("ID").equalTo(orc.col("ID_B")))
      .select("ID", "VALUE", "VALUE_B")

    assertRoundTripData(both)
  }

  test("Struct from sub-queries") {
    val csv = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(Paths.get("src/test/resources/dataset-a.csv").toAbsolutePath.toString)

    csv.createOrReplaceTempView("csv")
    val data = spark.sql("""
                           |select
                           |   (select sum(ID) from csv) sum,
                           |   (select count(ID) from csv) count
                           |
                           |""".stripMargin)

    val result = assertRoundTripData(data)
    assertResult(Row(55, 10))(result.head())
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

  test("Insert") {
    // create an in-memory table with 2 rows
    spark.sql("drop table if exists test")
    spark.sql("create table test(ID int, VALUE string)")
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

  test("Append to CSV file") {
    val tempFile = "build/tmp/test/write-a.csv"

    // create a CSV file with 2 rows
    val encoder = Encoders.tuple(Encoders.scalaInt, Encoders.STRING)
    val ds = spark.createDataset(Seq((1, "a"), (2, "b")))(encoder)
    ds.write.mode("overwrite").format("csv").save(tempFile)

    csvRead(tempFile).createOrReplaceTempView("csv")
    val insert = spark.sql("insert into csv values (3, 'c')")

    // there are 3 rows in the file
    assertResult(3)(csvRead(tempFile).count())

    // convert the plan to Substrait and back
    val plan = assertRoundTripExtension(insert.queryExecution.optimizedPlan)

    // this should not have affected the csv file (still 3 rows)
    assertResult(3)(csvRead(tempFile).count())

    // now execute the round-tripped plan and assert an extra row is appended
    spark.sessionState.executePlan(plan).executedPlan.execute()
    assertResult(4)(csvRead(tempFile).count())
    // and again...
    spark.sessionState.executePlan(plan).executedPlan.execute()
    assertResult(5)(csvRead(tempFile).count())
  }

  test("Create table as select") {
    spark.sql("drop table if exists ctas")
    val df = spark.sql(
      "create table ctas as select * from (values (1, 'a'), (2, 'b') as table(col1, col2))")
    assertResult(2)(spark.sql("select * from ctas").count())

    spark.sql("drop table ctas")

    val plan = assertRoundTripExtension(df.queryExecution.optimizedPlan)
    spark.sessionState.executePlan(plan).executedPlan.execute()
    assertResult(2)(spark.sql("select * from ctas").count())
  }

  private def csvRead(path: String): Dataset[Row] =
    spark.read
      .option("inferSchema", true)
      .csv(Paths.get(path).toAbsolutePath.toString)
}
