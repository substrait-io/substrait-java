package io.substrait.spark

import io.substrait.spark.compat.SparkCompat
import io.substrait.spark.logical.{ToLogicalPlan, ToSubstraitRel}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, EqualTo, Expression, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, GlobalLimit, LocalLimit, LogicalPlan, Project, Sort, Union}
import org.apache.spark.sql.classic.DatasetUtil
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, LogicalRelation, PartitionDirectory}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType, StringType, StructField, StructType}

import io.substrait.expression.ExpressionCreator
import io.substrait.plan.{PlanProtoConverter, ProtoPlanConverter}
import io.substrait.relation.{LocalFiles => SubstraitLocalFiles, Project => SubstraitProject, Set => SubstraitSet}
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
    assertResult(plan.schema.map(field => (field.name, field.dataType))) {
      converted.schema.map(field => (field.name, field.dataType))
    }
    val actual = DatasetUtil.fromLogicalPlan(spark, converted).collect().toSeq
    assertResult(expected.sortBy(_.toString))(actual.sortBy(_.toString))

    val bare = new ToLogicalPlan(spark).convert(new ToSubstraitRel().visit(plan))
    assertResult(plan.schema.map(field => (field.name, field.dataType))) {
      bare.schema.map(field => (field.name, field.dataType))
    }
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

  test("an unpartitioned zero-byte CSV file round-trips as an empty table") {
    withTempPath {
      directory =>
        val path = directory.getAbsolutePath
        spark.sql("select 1 id where false").coalesce(1).write.csv(path)
        val data = spark.read.schema("id INT").csv(path)
        assert(data.inputFiles.nonEmpty)
        assertRoundTrip(data.queryExecution.optimizedPlan, Seq.empty)
    }
  }

  test("an unpartitioned directory without data files round-trips as an empty table") {
    withTempPath {
      directory =>
        assert(directory.mkdirs())
        val data = spark.read.schema("id INT").csv(directory.getAbsolutePath)
        assertRoundTrip(data.queryExecution.optimizedPlan, Seq.empty)
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

  test("mixed-case overlapping columns reject only filters on the overlap") {
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
            assertRoundTrip(plan, Seq(Row(1, 999, "physical")))
            assertRoundTrip(data.select("id").queryExecution.optimizedPlan, Seq(Row(1)))
            assertRoundTrip(
              data.select("id", "value").queryExecution.optimizedPlan,
              Seq(Row(1, "physical")))
            val error = intercept[UnsupportedOperationException] {
              new ToSubstraitRel().convert(data.filter("p = 10").queryExecution.optimizedPlan)
            }
            assert(error.getMessage.contains("overlapping partition columns"))
            val alias = Alias(plan.output(1), "aliased")()
            val projected = Project(Seq(plan.output.head, alias), plan)
            val sorted = Sort(Seq(SortOrder(projected.output.head, Ascending)), true, projected)
            Seq(projected, sorted).foreach {
              child =>
                val filtered = Filter(EqualTo(alias.toAttribute, Literal(10)), child)
                intercept[UnsupportedOperationException](new ToSubstraitRel().convert(filtered))
            }
            val union = Union(Seq(plan, plan), byName = false, allowMissingCol = false)
            intercept[UnsupportedOperationException] {
              new ToSubstraitRel().convert(Filter(EqualTo(union.output(1), Literal(10)), union))
            }
            val limited = GlobalLimit(Literal(1), LocalLimit(Literal(1), plan))
            assertRoundTrip(Filter(EqualTo(limited.output(1), Literal(10)), limited), Seq.empty)
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
        val emptyDirectory =
          PartitionDirectory(InternalRow(30), Nil)
        val withEmpty = withPartitions(original, selected :+ emptyDirectory)
        assertRoundTrip(withEmpty, Seq(Row(1, 10)))
        assert(new ToSubstraitRel().visit(withEmpty).isInstanceOf[SubstraitProject])
        assertRoundTrip(withPartitions(original, Seq.empty), Seq.empty)
    }
  }

  test("partition projects import as one scan and partition filters prune exported files") {
    withTempPath {
      directory =>
        val path = directory.getAbsolutePath
        spark
          .range(8)
          .selectExpr("id", "cast(id as int) part")
          .write
          .partitionBy("part")
          .parquet(path)
        val data = spark.read.parquet(path)
        val exported = new ToSubstraitRel().visit(data.queryExecution.optimizedPlan)
        assert(exported.isInstanceOf[SubstraitSet])
        val imported = new ToLogicalPlan(spark).convert(exported)
        assertResult(1)(imported.collect { case _: LogicalRelation => 1 }.size)
        assertResult(data.collect().toSeq.sortBy(_.toString)) {
          DatasetUtil.fromLogicalPlan(spark, imported).collect().toSeq.sortBy(_.toString)
        }
        assertResult(Seq(Row(7L, 7))) {
          DatasetUtil.fromLogicalPlan(spark, imported).filter("part = 7").collect().toSeq
        }
        val filtered = new ToSubstraitRel()
          .visit(data.filter("part = 7").queryExecution.optimizedPlan)
          .asInstanceOf[io.substrait.relation.Filter]
        assert(filtered.getInput.isInstanceOf[SubstraitProject])
        assertRoundTrip(data.filter("part = 7").queryExecution.optimizedPlan, Seq(Row(7L, 7)))
    }
  }

  test("raw paths preserve URI punctuation and folder URIs normalize trailing slashes") {
    Seq("a%3Ab", "a#b", "a?b").foreach {
      name =>
        withTempPath {
          directory =>
            val path = new java.io.File(directory, name)
            spark.sql("select 1 id").write.parquet(path.getAbsolutePath)
            val original = spark.read.parquet(path.getAbsolutePath).queryExecution.optimizedPlan
            val scan = new ToSubstraitRel().visit(original).asInstanceOf[SubstraitLocalFiles]
            val items = scan.getItems.asScala
              .map(
                file =>
                  FileOrFiles
                    .builder()
                    .from(file)
                    .path(new URI(file.getPath.get()).getPath)
                    .build())
              .toSeq
            val raw = SubstraitLocalFiles.builder().from(scan).items(items.asJava).build()
            assertResult(Seq(Row(1)))(
              DatasetUtil
                .fromLogicalPlan(spark, new ToLogicalPlan(spark).convert(raw))
                .collect()
                .toSeq)
            val folder = FileOrFiles
              .builder()
              .from(scan.getItems.get(0))
              .path(path.toURI.toString)
              .pathType(FileOrFiles.PathType.URI_FOLDER)
              .build()
            val folderScan =
              SubstraitLocalFiles.builder().from(scan).items(Seq(folder).asJava).build()
            assertResult(Seq(Row(1)))(
              DatasetUtil
                .fromLogicalPlan(spark, new ToLogicalPlan(spark).convert(folderScan))
                .collect()
                .toSeq)
            val empty = SubstraitLocalFiles
              .builder()
              .from(scan)
              .items(Seq(FileOrFiles.builder().from(folder).path("").build()).asJava)
              .build()
            intercept[IllegalArgumentException](new ToLogicalPlan(spark).convert(empty))
        }
    }
  }

  test("complex literal projects remain separate scans on import") {
    withTempPath {
      directory =>
        val path = directory.getAbsolutePath
        spark.sql("select 1 id").write.parquet(path)
        val scan = new ToSubstraitRel().visit(spark.read.parquet(path).queryExecution.optimizedPlan)
        val literal = ExpressionCreator.list(false, ExpressionCreator.i32(false, 7))
        val project = SubstraitProject.builder().input(scan).addExpressions(literal).build()
        val union = SubstraitSet
          .builder()
          .setOp(SubstraitSet.SetOp.UNION_ALL)
          .addInputs(project, project)
          .build()
        val imported = new ToLogicalPlan(spark).convert(union)
        assertResult(2)(imported.collect { case _: LogicalRelation => 1 }.size)
        assertResult(Seq(Row(1, Seq(7)), Row(1, Seq(7)))) {
          DatasetUtil.fromLogicalPlan(spark, imported).collect().toSeq
        }
    }
  }

  test("partition decimals are rounded and overflow to null using their declared type") {
    // Spark's vectorized reader reinterprets the unscaled directory value instead of fitting it.
    withSQLConf("spark.sql.parquet.enableVectorizedReader" -> "false") {
      Seq(
        ("1.25", DecimalType(10, 1), new java.math.BigDecimal("1.3")),
        ("123.4", DecimalType(3, 1), null)).foreach {
        case (value, decimalType, expected) =>
          withTempPath {
            directory =>
              val path = directory.getAbsolutePath
              spark.sql("select 1 id").write.parquet(s"$path/part=$value")
              val data = spark.read
                .schema(
                  StructType(Seq(StructField("id", IntegerType), StructField("part", decimalType))))
                .parquet(path)
              assertRoundTrip(data.queryExecution.optimizedPlan, Seq(Row(1, expected)))
          }
      }
    }
  }

  test("partition mapping uses the captured merged schema and Unicode names") {
    withSQLConf("spark.sql.caseSensitive" -> "false ") {
      withTempPath {
        directory =>
          val path = directory.getAbsolutePath
          spark.sql("select 1 id, 999 `İl`").write.parquet(s"$path/il=34")
          val data = spark.read.parquet(path)
          assertRoundTrip(data.queryExecution.optimizedPlan, Seq(Row(1, 999, 34)))
      }
    }
    withTempPath {
      directory =>
        val path = directory.getAbsolutePath
        spark.sql("select 1 id, 999 P").write.parquet(s"$path/p=10")
        withSQLConf("spark.sql.caseSensitive" -> "true") {
          val plan = spark.read.parquet(path).queryExecution.optimizedPlan
          withSQLConf("spark.sql.caseSensitive" -> "false") {
            val read = new ToSubstraitRel().visit(plan).asInstanceOf[SubstraitProject]
            assertResult(Seq(0, 1, 2))(
              read.getRemap.get().indices().asScala.map(_.intValue()).toSeq)
          }
        }
    }
  }
}
