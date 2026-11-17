package io.substrait.spark

import io.substrait.spark.logical.{ToLogicalPlan, ToSubstraitRel}

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.classic.DatasetUtil
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation, InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.test.SharedSparkSession

import org.apache.hadoop.fs.Path

class FileFormatSuite extends SharedSparkSession {

  Seq(
    ("parquet", () => new ParquetFileFormat() {}),
    ("orc", () => new OrcFileFormat() {}),
    ("csv", () => new CSVFileFormat() {})
  ).foreach {
    case (format, subclass) =>
      Seq(false, true).foreach {
        partitioned =>
          test(
            s"reject $format subclasses on ${if (partitioned) "partitioned" else "unpartitioned"} reads") {
            withTempPath {
              directory =>
                val path = directory.getAbsolutePath
                val writer = spark.sql("SELECT 1 AS id, 10 AS part").write.format(format)
                if (partitioned) writer.partitionBy("part").save(path) else writer.save(path)
                val data = spark.read.format(format).schema("id INT, part INT").load(path)
                val logical = data.queryExecution.optimizedPlan.collect {
                  case relation: LogicalRelation => relation
                }.head
                val relation = logical.relation.asInstanceOf[HadoopFsRelation]
                val unsupported = subclass(): FileFormat
                val plan = logical.copy(relation = relation.copy(fileFormat = unsupported)(spark))
                val error = intercept[UnsupportedOperationException] {
                  new ToSubstraitRel().convert(plan)
                }
                assert(error.getMessage.contains("File format not currently supported"))
                assert(error.getMessage.contains(unsupported.getClass.getName))
            }
          }
      }

      test(s"empty $format sources still reject subclasses") {
        withTempPath {
          directory =>
            assert(directory.mkdirs())
            val data = spark.read.format(format).schema("id INT").load(directory.getAbsolutePath)
            val logical = data.queryExecution.optimizedPlan.collect {
              case relation: LogicalRelation => relation
            }.head
            val relation = logical.relation.asInstanceOf[HadoopFsRelation]
            assert(relation.partitionSchema.isEmpty)
            assert(relation.location.listFiles(Nil, Nil).flatMap(_.files).isEmpty)
            val plain = new ToSubstraitRel().convert(logical)
            val converted = new ToLogicalPlan(spark).convert(plain)
            assert(DatasetUtil.fromLogicalPlan(spark, converted).collect().isEmpty)

            val unsupported = subclass(): FileFormat
            val plan = logical.copy(relation = relation.copy(fileFormat = unsupported)(spark))
            val error = intercept[UnsupportedOperationException] {
              new ToSubstraitRel().convert(plan)
            }
            assert(error.getMessage.contains(unsupported.getClass.getName))
        }
      }

      test(s"reject $format subclasses on file writes") {
        withTempPath {
          directory =>
            val child = spark.sql("SELECT 1 AS id").queryExecution.optimizedPlan
            val unsupported = subclass(): FileFormat
            val command = InsertIntoHadoopFsRelationCommand(
              outputPath = new Path(directory.getAbsolutePath),
              staticPartitions = Map.empty,
              ifPartitionNotExists = false,
              partitionColumns = Seq.empty,
              bucketSpec = None,
              fileFormat = unsupported,
              options = Map.empty,
              query = child,
              mode = SaveMode.Append,
              catalogTable = None,
              fileIndex = None,
              outputColumnNames = Seq("id")
            )
            val error = intercept[UnsupportedOperationException] {
              new ToSubstraitRel().visit(command)
            }
            assert(error.getMessage.contains(unsupported.getClass.getName))
        }
      }

      test(s"plain $format reads still round-trip") {
        withTempPath {
          directory =>
            val path = directory.getAbsolutePath
            spark.sql("SELECT 1 AS id, 10 AS part").write.format(format).save(path)
            val data = spark.read.format(format).schema("id INT, part INT").load(path)
            val plan = new ToSubstraitRel().convert(data.queryExecution.optimizedPlan)
            val converted = new ToLogicalPlan(spark).convert(plan)
            assertResult(Seq(Row(1, 10))) {
              DatasetUtil.fromLogicalPlan(spark, converted).collect().toSeq
            }
        }
      }
  }
}
