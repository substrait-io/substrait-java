package io.substrait.spark

import io.substrait.spark.compat.SparkCompat
import io.substrait.spark.logical.{ToLogicalPlan, ToSubstraitRel}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.DatasetUtil
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, LogicalRelation, PartitionDirectory}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructField, StructType}

import io.substrait.plan.{PlanProtoConverter, ProtoPlanConverter}
import io.substrait.relation.{LocalFiles => SubstraitLocalFiles}
import io.substrait.relation.files.FileOrFiles
import org.apache.hadoop.fs.Path

import java.net.URI
import java.time.LocalDate

import scala.jdk.CollectionConverters._

class PartitionedFilesSuite extends SharedSparkSession {

  private def assertRoundTrip(plan: LogicalPlan, expected: Seq[Row]): Unit = {
    val original = DatasetUtil.fromLogicalPlan(spark, plan).collect().toSeq
    assertResult(expected.sortBy(_.toString))(original.sortBy(_.toString))

    val substrait = new ToSubstraitRel().convert(plan)
    val bytes = new PlanProtoConverter().toProto(substrait).toByteArray
    val decoded = new ProtoPlanConverter().from(io.substrait.proto.Plan.parseFrom(bytes))
    assertResult(substrait)(decoded)

    val converted = new ToLogicalPlan(spark).convert(decoded)
    assert(
      DataType.equalsStructurallyByName(plan.schema, converted.schema, caseSensitiveResolution))
    val actual = DatasetUtil.fromLogicalPlan(spark, converted).collect().toSeq
    assertResult(expected.sortBy(_.toString))(actual.sortBy(_.toString))
  }

  Seq("parquet", "orc", "csv").foreach {
    format =>
      test(s"partition values survive $format reads and file options") {
        withTempPath {
          directory =>
            val path = directory.getAbsolutePath
            spark
              .sql("select 1 id, 'left|right' value, 10 part union all select 2, 'other', 20")
              .write
              .format(format)
              .option("header", true)
              .option("delimiter", "|")
              .partitionBy("part")
              .save(path)
            val schema = StructType(
              Seq(
                StructField("id", IntegerType),
                StructField("value", StringType),
                StructField("part", IntegerType)))
            val data = spark.read
              .format(format)
              .schema(schema)
              .option("header", true)
              .option("delimiter", "|")
              .load(path)
            assertRoundTrip(
              data.queryExecution.optimizedPlan,
              Seq(Row(1, "left|right", 10), Row(2, "other", 20)))
        }
      }
  }

  test("multiple roots and basePath preserve the selected partition values") {
    withTempPath {
      directory =>
        val path = directory.getAbsolutePath
        spark
          .sql("select 1 id, 10 part union all select 2, 20 union all select 3, 30")
          .write
          .partitionBy("part")
          .parquet(path)
        val selected = spark.read
          .option("basePath", path)
          .parquet(s"$path/part=10", s"$path/part=20")
        assertRoundTrip(selected.queryExecution.optimizedPlan, Seq(Row(1, 10), Row(2, 20)))
        assertRoundTrip(selected.filter("part = 10").queryExecution.optimizedPlan, Seq(Row(1, 10)))
    }
  }

  test("date null and escaped string partition values retain their types and values") {
    withSQLConf("spark.sql.datetime.java8API.enabled" -> "true") {
      withTempPath {
        directory =>
          val path = directory.getAbsolutePath + "/root with spaces"
          spark
            .sql("select 1 id, date '2024-01-02' day, 'a/b% c' label " +
              "union all select 2, cast(null as date), cast(null as string)")
            .write
            .partitionBy("day", "label")
            .parquet(path)
          val data = spark.read.parquet(path)
          assertRoundTrip(
            data.queryExecution.optimizedPlan,
            Seq(Row(1, LocalDate.of(2024, 1, 2), "a/b% c"), Row(2, null, null)))
      }
    }
  }

  test("local file reads still accept unescaped paths containing spaces") {
    withTempPath {
      directory =>
        val path = directory.getAbsolutePath + "/root with spaces"
        spark.sql("select 1 id").write.parquet(path)
        val original = spark.read.parquet(path).queryExecution.optimizedPlan
        val scan = new ToSubstraitRel().visit(original).asInstanceOf[SubstraitLocalFiles]
        val files = scan.getItems.asScala.map {
          file => FileOrFiles.builder().from(file).path(new URI(file.getPath.get()).getPath).build()
        }
        val rawPaths = SubstraitLocalFiles.builder().from(scan).items(files.toSeq.asJava).build()
        val converted = new ToLogicalPlan(spark).convert(rawPaths)
        assertResult(Seq(Row(1)))(DatasetUtil.fromLogicalPlan(spark, converted).collect().toSeq)
    }
  }

  test("explicit partition types are retained") {
    withTempPath {
      directory =>
        val path = directory.getAbsolutePath
        spark.sql("select 1 id, 10 part").write.partitionBy("part").parquet(path)
        val schema = StructType(Seq(StructField("id", IntegerType), StructField("part", LongType)))
        val data = spark.read.schema(schema).option("basePath", path).parquet(s"$path/part=10")
        assertRoundTrip(data.queryExecution.optimizedPlan, Seq(Row(1, 10L)))
    }
  }

  test("partition values override overlapping file columns in merged schema order") {
    withSQLConf("spark.sql.caseSensitive" -> "false") {
      withTempPath {
        directory =>
          val path = directory.getAbsolutePath
          spark.sql("select 1 id, '999' p, 'physical' value").write.parquet(s"$path/p=10")
          val data = spark.read.parquet(path)
          assertResult(Seq("id", "p", "value"))(data.columns.toSeq)
          assertRoundTrip(data.queryExecution.optimizedPlan, Seq(Row(1, 10, "physical")))
      }
    }
  }

  test("mixed-case overlapping columns are rejected when Spark does not override them") {
    withSQLConf("spark.sql.caseSensitive" -> "false") {
      withTempPath {
        directory =>
          val path = directory.getAbsolutePath
          spark.sql("select 1 id, 999 P, 'physical' value").write.parquet(s"$path/p=10")
          val data = spark.read.parquet(path)
          val plan = data.queryExecution.optimizedPlan
          if (SparkCompat.instance.supportsCaseInsensitivePartitionOverlap) {
            assertRoundTrip(plan, Seq(Row(1, 10, "physical")))
          } else {
            assertResult(Seq(Row(1, 999, "physical")))(data.collect().toSeq)
            val error = intercept[UnsupportedOperationException] {
              new ToSubstraitRel().convert(plan)
            }
            assert(error.getMessage.contains("differ only in case"))
          }
      }
    }
  }

  test("case-sensitive file and partition column names remain distinct") {
    withSQLConf("spark.sql.caseSensitive" -> "true") {
      withTempPath {
        directory =>
          val path = directory.getAbsolutePath
          spark.sql("select 1 id, 999 P, 'physical' value").write.parquet(s"$path/p=10")
          val data = spark.read.parquet(path)
          assertRoundTrip(data.queryExecution.optimizedPlan, Seq(Row(1, 999, "physical", 10)))
      }
    }
  }

  private def withPartitions(
      original: HadoopFsRelation,
      partitions: Seq[PartitionDirectory]): LogicalPlan = {
    val index = new FileIndex {
      override def rootPaths: Seq[Path] = original.location.rootPaths
      override def listFiles(
          partitionFilters: Seq[Expression],
          dataFilters: Seq[Expression]): Seq[PartitionDirectory] = partitions
      override def inputFiles: Array[String] =
        partitions.flatMap(_.files.map(_.getPath.toString)).toArray
      override def refresh(): Unit = ()
      override def sizeInBytes: Long = partitions.flatMap(_.files.map(_.getLen)).sum
      override def partitionSchema: StructType = original.partitionSchema
    }
    val relation = original.copy(location = index)(spark)
    SparkCompat.instance.createLogicalRelation(
      relation,
      ToSparkType.toAttributeSeq(ToSubstraitType.toNamedStruct(relation.schema)),
      None,
      false)
  }

  test("pruned and empty file indexes do not restore excluded partitions") {
    withTempPath {
      directory =>
        val path = directory.getAbsolutePath
        spark
          .sql("select 1 id, 10 part union all select 2, 20")
          .write
          .partitionBy("part")
          .parquet(path)
        val logical = spark.read
          .parquet(path)
          .queryExecution
          .optimizedPlan
          .asInstanceOf[LogicalRelation]
        val original = logical.relation.asInstanceOf[HadoopFsRelation]
        val selected = original.location.listFiles(Nil, Nil).filter(_.values.getInt(0) == 10)
        assertRoundTrip(withPartitions(original, selected), Seq(Row(1, 10)))
        assertRoundTrip(withPartitions(original, Seq.empty), Seq.empty)
    }
  }
}
