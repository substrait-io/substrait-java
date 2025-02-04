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

import org.apache.spark.sql.{Dataset, DatasetUtil, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import io.substrait.plan.{PlanProtoConverter, ProtoPlanConverter}

import java.nio.file.Paths

class LocalFiles extends SharedSparkSession {
  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext.setLogLevel("WARN")

    conf.setConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED, false)
    // introduced in spark 3.4
    spark.conf.set("spark.sql.readSideCharPadding", "false")
  }

  def assertRoundTrip(data: Dataset[Row]): Dataset[Row] = {
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

    val sparkPlan2 = new ToLogicalPlan(spark).convert(substraitPlan2)
    val result = DatasetUtil.fromLogicalPlan(spark, sparkPlan2)

    assertResult(data.columns)(result.columns)
    assertResult(data.count)(result.count)
    data.collect().zip(result.collect()).foreach {
      case (before, after) => assertResult(before)(after)
    }
    result
  }

  test("CSV with header") {
    val table = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(Paths.get("src/test/resources/dataset-a.csv").toAbsolutePath.toString)

    assertRoundTrip(table)
  }

  test("CSV null value") {
    val table = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .option("nullValue", "seven")
      .csv(Paths.get("src/test/resources/dataset-a.csv").toAbsolutePath.toString)

    val result = assertRoundTrip(table)
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

    assertRoundTrip(table)
  }

  test("Read csv folder") {
    val table = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(Paths.get("src/test/resources/csv/").toAbsolutePath.toString)

    assertRoundTrip(table)
  }

  test("Read parquet file") {
    val table = spark.read
      .parquet(Paths.get("src/test/resources/dataset-a.parquet").toAbsolutePath.toString)

    assertRoundTrip(table)
  }

  test("Read orc file") {
    val table = spark.read
      .orc(Paths.get("src/test/resources/dataset-a.orc").toAbsolutePath.toString)

    assertRoundTrip(table)
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

    assertRoundTrip(both)
  }
}
